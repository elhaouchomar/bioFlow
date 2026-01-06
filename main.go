package main

import (
	"archive/zip"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq"
)

// --- Constants ---
const (
	IMG_FASTQC  = "staphb/fastqc:latest"
	IMG_SHOVILL = "staphb/shovill:latest"
	IMG_QUAST   = "staphb/quast:latest"
	IMG_PROKKA  = "staphb/prokka:latest"
	IMG_AMR     = "staphb/ncbi-amrfinderplus:latest"
	IMG_PLASMID = "staphb/plasmidfinder:latest"

	TypeBioJob = "bio:pipeline_job"
)

var redisAddr string

// --- Data Structures ---
type JobPayload struct {
	JobID     string `json:"job_id"`
	InputType string `json:"input_type"`
	FileR1    string `json:"file_r1,omitempty"`
	FileR2    string `json:"file_r2,omitempty"`
	FileFasta string `json:"file_fasta,omitempty"`
}

type JobStatus struct {
	State       string `json:"state"`
	Progress    int    `json:"progress"`
	CurrentStep string `json:"current_step"`
	Error       string `json:"error,omitempty"`
}

type FinalReport struct {
	JobID     string `json:"job_id"`
	Status    string `json:"status"`
	InputType string `json:"input_type"`
	Timestamp string `json:"timestamp"`
	Assembly  struct {
		TotalLength string `json:"total_length"`
		N50         string `json:"n50"`
		GC          string `json:"gc"`
		Contigs     string `json:"contigs"`
	} `json:"assembly"`
}

func main() {
	mode := flag.String("mode", "server", "Mode: 'worker' or 'server'")
	flag.Parse()

	redisAddr = os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	if *mode == "worker" {
		runWorker(redisAddr)
	} else {
		runServer(redisAddr)
	}
}

// ---------------------------------------------------------
// 1. WEB SERVER
// ---------------------------------------------------------
func runServer(redisAddr string) {
	workDir, _ := filepath.Abs("workspace/jobs")
	fs := http.FileServer(http.Dir(workDir))

	http.Handle("/download/", http.StripPrefix("/download/", fs))
	http.HandleFunc("/", serveHTML)
	http.HandleFunc("/upload", handleUpload)
	http.HandleFunc("/status", handleStatus)
	http.HandleFunc("/download-zip", handleDownloadZip)

	fmt.Println("🌐 Server started at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleDownloadZip(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	workDir, _ := filepath.Abs("workspace")
	zipPath := filepath.Join(workDir, "jobs", jobID, "output", "results.zip")

	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		http.Error(w, "ZIP file not ready yet", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s_results.zip\"", jobID))
	http.ServeFile(w, r, zipPath)
}

func serveHTML(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "templates/index.html")
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	r.ParseMultipartForm(500 << 20)

	inputType := r.FormValue("type")
	jobID := fmt.Sprintf("job_%d", time.Now().Unix())

	localWorkDir, _ := filepath.Abs("workspace")
	jobDir := filepath.Join(localWorkDir, "jobs", jobID, "input")
	os.MkdirAll(jobDir, 0777)
	os.Chmod(filepath.Join(localWorkDir, "jobs", jobID), 0777)

	updateJobStatus(jobID, "running", 0, "Initializing Upload...")

	var p JobPayload
	p.JobID = jobID
	p.InputType = inputType

	if inputType == "reads" {
		p.FileR1 = saveFile(r, "r1", jobDir)
		p.FileR2 = saveFile(r, "r2", jobDir)
	} else {
		p.FileFasta = saveFile(r, "fasta", jobDir)
	}

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer client.Close()
	data, _ := json.Marshal(p)
	client.Enqueue(asynq.NewTask(TypeBioJob, data))

	updateJobStatus(jobID, "running", 5, "Queued for processing")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "success", "job_id": jobID})
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job_id")
	localWorkDir, _ := filepath.Abs("workspace")
	statusFile := filepath.Join(localWorkDir, "jobs", jobID, "status.json")

	w.Header().Set("Content-Type", "application/json")

	if content, err := os.ReadFile(statusFile); err == nil {
		w.Write(content)
	} else {
		json.NewEncoder(w).Encode(JobStatus{State: "unknown", Progress: 0})
	}
}

func updateJobStatus(jobID, state string, progress int, step string) {
	localWorkDir, _ := filepath.Abs("workspace")
	statusFile := filepath.Join(localWorkDir, "jobs", jobID, "status.json")

	status := JobStatus{State: state, Progress: progress, CurrentStep: step}
	bytes, _ := json.Marshal(status)
	os.WriteFile(statusFile, bytes, 0644)
	os.Chmod(statusFile, 0666)
}

func saveFile(r *http.Request, field, dir string) string {
	file, header, err := r.FormFile(field)
	if err != nil { return "" }
	defer file.Close()
	path := filepath.Join(dir, header.Filename)
	out, _ := os.Create(path)
	defer out.Close()
	io.Copy(out, file)
	rel, _ := filepath.Rel("/app/workspace", path)
	return rel
}

// ---------------------------------------------------------
// 2. WORKER LOGIC
// ---------------------------------------------------------
func runWorker(redisAddr string) {
	srv := asynq.NewServer(asynq.RedisClientOpt{Addr: redisAddr}, asynq.Config{Concurrency: 2})
	mux := asynq.NewServeMux()
	mux.HandleFunc(TypeBioJob, handleBioJob)
	fmt.Println(" [*] Worker started...")
	srv.Run(mux)
}

func decompressGZ(inputPath, outputPath string) error {
	gzFile, err := os.Open(inputPath)
	if err != nil { return err }
	defer gzFile.Close()
	gzReader, err := gzip.NewReader(gzFile)
	if err != nil { return err }
	defer gzReader.Close()
	outFile, err := os.Create(outputPath)
	if err != nil { return err }
	defer outFile.Close()
	_, err = io.Copy(outFile, gzReader)
	return err
}

func handleBioJob(ctx context.Context, t *asynq.Task) error {
	var p JobPayload
	json.Unmarshal(t.Payload(), &p)

	updateStatus := func(prog int, step string) {
		updateJobStatus(p.JobID, "running", prog, step)
	}

	workDir, _ := filepath.Abs("workspace")
	jobDir := filepath.Join(workDir, "jobs", p.JobID)
	outDir := filepath.Join(jobDir, "output")
	inputDir := filepath.Join(jobDir, "input")
	os.MkdirAll(outDir, 0777)
	os.MkdirAll(inputDir, 0777)

	dockerOut := fmt.Sprintf("/workspace/jobs/%s/output", p.JobID)
	var contigs string
	var filesToCleanup []string

	// --- Step 1: QC & Assembly ---
	if p.InputType == "reads" {
		r1Path := filepath.Join(workDir, p.FileR1)
		r2Path := filepath.Join(workDir, p.FileR2)

		if strings.HasSuffix(r1Path, ".gz") {
			updateStatus(10, "Decompressing R1...")
			decompressedR1 := strings.TrimSuffix(r1Path, ".gz")
			decompressGZ(r1Path, decompressedR1)
			filesToCleanup = append(filesToCleanup, decompressedR1)
		}
		if strings.HasSuffix(r2Path, ".gz") {
			updateStatus(12, "Decompressing R2...")
			decompressedR2 := strings.TrimSuffix(r2Path, ".gz")
			decompressGZ(r2Path, decompressedR2)
			filesToCleanup = append(filesToCleanup, decompressedR2)
		}

		dockerR1 := filepath.Join("/workspace", strings.TrimSuffix(p.FileR1, ".gz"))
		dockerR2 := filepath.Join("/workspace", strings.TrimSuffix(p.FileR2, ".gz"))

		updateStatus(15, "Quality Control (FastQC)...")
		runDocker(IMG_FASTQC, "fastqc", dockerR1, dockerR2, "-o", filepath.Join(dockerOut, "fastqc"))

		// --- OPTIMIZATION 1: Use Megahit (Faster than Spades) ---
		updateStatus(30, "Assembly (Shovill/Megahit)...")
		err := runDocker(IMG_SHOVILL, "shovill",
			"--R1", dockerR1,
			"--R2", dockerR2,
			"--outdir", filepath.Join(dockerOut, "shovill"),
			"--assembler", "megahit", // 🚀 KEY CHANGE: Much faster assembler
			"--force", "--cpus", "4", "--ram", "8")
		
		if err != nil {
			updateJobStatus(p.JobID, "error", 0, "Assembly Failed")
			return err
		}

		exec.Command("cp", filepath.Join(outDir, "shovill", "contigs.fa"), filepath.Join(outDir, "contigs.fa")).Run()
		contigs = filepath.Join(dockerOut, "contigs.fa")

	} else {
		updateStatus(10, "Processing Input Genome...")
		fastaPath := filepath.Join(workDir, p.FileFasta)
		dstContigs := filepath.Join(outDir, "contigs.fa")
		if strings.HasSuffix(fastaPath, ".gz") {
			decompressGZ(fastaPath, dstContigs)
		} else {
			exec.Command("cp", fastaPath, dstContigs).Run()
		}
		contigs = filepath.Join(dockerOut, "contigs.fa")
	}

	if _, err := os.Stat(filepath.Join(outDir, "contigs.fa")); os.IsNotExist(err) {
		updateJobStatus(p.JobID, "error", 0, "Assembly Failed - No contigs found")
		return fmt.Errorf("assembly failed")
	}

	// --- Step 2: Annotation ---
	// --- OPTIMIZATION 2: Use --fast flag for Prokka ---
	updateStatus(60, "Annotation (Prokka Fast)...")
	runDocker(IMG_PROKKA, "prokka", "--outdir", filepath.Join(dockerOut, "prokka"), 
		"--force", "--prefix", "genome", 
		"--fast", // 🚀 KEY CHANGE: Skips HMM search (Much faster)
		contigs)

	// --- Step 3: Analysis ---
	updateStatus(80, "Parallel Analysis...")
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); runDocker(IMG_QUAST, "quast.py", contigs, "-o", filepath.Join(dockerOut, "quast")) }()
	go func() { defer wg.Done(); runDocker(IMG_AMR, "amrfinder", "-n", contigs, "-o", filepath.Join(dockerOut, "amr", "amr_results.tsv")) }()
	go func() { defer wg.Done(); runDocker(IMG_PLASMID, "plasmidfinder.py", "-i", contigs, "-o", filepath.Join(dockerOut, "plasmid")) }()
	wg.Wait()

	// --- Step 4 & 5 ---
	updateStatus(90, "Generating Reports...")
	generateFinalReport(p, outDir)
	createZipArchive(outDir, filepath.Join(outDir, "results.zip"))

	for _, f := range filesToCleanup { os.Remove(f) }

	updateJobStatus(p.JobID, "completed", 100, "Analysis Complete")
	return nil
}

func createZipArchive(sourceDir, zipPath string) error {
	zipFile, err := os.Create(zipPath)
	if err != nil { return err }
	defer zipFile.Close()
	archive := zip.NewWriter(zipFile)
	defer archive.Close()
	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || path == zipPath { return nil }
		relPath, _ := filepath.Rel(sourceDir, path)
		w, _ := archive.Create(filepath.ToSlash(relPath))
		f, _ := os.Open(path)
		io.Copy(w, f)
		f.Close()
		return nil
	})
}

func runDocker(img string, args ...string) error {
	host, _ := os.Getwd()
	if h := os.Getenv("HOST_WORKSPACE"); h != "" { host = h }
	cmdArgs := append([]string{"run", "--rm", "-v", fmt.Sprintf("%s:/workspace", host), "-w", "/workspace", img}, args...)
	cmd := exec.Command("docker", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Docker Error [%s]: %s", img, string(output))
		return err
	}
	return nil
}

func generateFinalReport(p JobPayload, dir string) {
	r := FinalReport{
		JobID:     p.JobID,
		Status:    "completed",
		InputType: p.InputType,
		Timestamp: time.Now().Format(time.RFC1123),
	}
	if f, err := os.Open(filepath.Join(dir, "quast", "transposed_report.tsv")); err == nil {
		s := bufio.NewScanner(f); s.Scan(); s.Scan()
		parts := strings.Split(s.Text(), "\t")
		if len(parts) > 16 {
			r.Assembly.TotalLength = parts[1]; r.Assembly.N50 = parts[14]; r.Assembly.GC = parts[16]; r.Assembly.Contigs = parts[1]
		}
		f.Close()
	}
	jsonBytes, _ := json.MarshalIndent(r, "", "  ")
	os.WriteFile(filepath.Join(dir, "final_summary.json"), jsonBytes, 0644)
	tmplPath, _ := filepath.Abs("templates/report.html")
	if tmpl, err := template.ParseFiles(tmplPath); err == nil {
		f, _ := os.Create(filepath.Join(dir, "final_report.html"))
		tmpl.Execute(f, r)
		f.Close()
	}
}