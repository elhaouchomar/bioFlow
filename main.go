package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
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

// JobStatus لتتبع التقدم
type JobStatus struct {
	State       string `json:"state"`        // running, completed, error
	Progress    int    `json:"progress"`     // 0 to 100
	CurrentStep string `json:"current_step"` // e.g., "Quality Control", "Assembly"
	Error       string `json:"error,omitempty"`
}

type FinalReport struct {
	JobID      string `json:"job_id"`
	Status     string `json:"status"`
	InputType  string `json:"input_type"`
	Assembly   struct{ TotalLength, N50, GC, Contigs string } `json:"assembly"`
}

func main() {
	mode := flag.String("mode", "server", "Mode: 'worker' or 'server'")
	flag.Parse()

	redisAddr = os.Getenv("REDIS_ADDR")
	if redisAddr == "" { redisAddr = "localhost:6379" }

	if *mode == "worker" {
		runWorker(redisAddr)
	} else {
		runServer(redisAddr)
	}
}

// ---------------------------------------------------------
// 1. WEB SERVER (Frontend + API)
// ---------------------------------------------------------
func runServer(redisAddr string) {
	workDir, _ := filepath.Abs("workspace/jobs")
	fs := http.FileServer(http.Dir(workDir))
	

	http.Handle("/download/", http.StripPrefix("/download/", fs))

	http.HandleFunc("/", serveHTML)
	http.HandleFunc("/upload", handleUpload)
	http.HandleFunc("/status", handleStatus)

	fmt.Println("🌐 Server started at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func serveHTML(w http.ResponseWriter, r *http.Request) {
	html := `
	<!DOCTYPE html>
	<html>
	<head>
		<title>BioFlow Pipeline</title>
		<style>
			body { font-family: 'Segoe UI', sans-serif; max-width: 900px; margin: 40px auto; padding: 20px; background-color: #f8f9fa; }
			.card { background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-bottom: 25px; }
			h1 { color: #2c3e50; text-align: center; margin-bottom: 30px; }
			
			/* Progress Bar Styles */
			.progress-container { margin-top: 20px; display: none; }
			.progress-bar { background-color: #e9ecef; border-radius: 8px; overflow: hidden; height: 25px; margin-bottom: 10px; }
			.progress-fill { background-color: #3498db; height: 100%; width: 0%; transition: width 0.5s ease; text-align: center; color: white; line-height: 25px; font-size: 12px; font-weight: bold; }
			.step-text { text-align: center; font-weight: bold; color: #555; margin-bottom: 15px; }
			
			/* Steps Diagram (Simple CSS representation) */
			.steps-diagram { display: flex; justify-content: space-between; margin-top: 20px; font-size: 12px; color: #aaa; }
			.step-item { position: relative; width: 100%; text-align: center; }
			.step-item.active { color: #3498db; font-weight: bold; }
			.step-item.completed { color: #27ae60; }
			.step-item::before { content: '●'; display: block; font-size: 20px; margin-bottom: 5px; }
			.step-item.active::before { color: #3498db; }
			.step-item.completed::before { content: '✔'; color: #27ae60; }

			.btn { background: #3498db; color: white; padding: 12px 24px; border: none; border-radius: 6px; cursor: pointer; width: 100%; font-size: 16px; }
			.btn:disabled { background: #bdc3c7; }
			.download-btn { display: inline-block; background: #27ae60; color: white; text-decoration: none; padding: 10px 15px; border-radius: 5px; margin: 5px; font-size: 14px; }
			
			select, input { width: 100%; padding: 10px; margin-bottom: 15px; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box; }
		</style>
	</head>
	<body>
		<h1>Microbial Genomics Pipeline</h1>
		
		<div class="card">
			<h3>Upload Data</h3>
			<form id="uploadForm">
				<label>Analysis Type:</label>
				<select name="type" id="inputType">
					<option value="reads">Option 1: Raw NGS Reads (FASTQ)</option>
					<option value="genome">Option 2: Assembled Genome (FASTA)</option>
				</select>

				<div id="readsInput">
					<input type="file" name="r1" placeholder="R1.fastq">
					<input type="file" name="r2" placeholder="R2.fastq">
				</div>
				<div id="genomeInput" style="display:none;">
					<input type="file" name="fasta" placeholder="genome.fa">
				</div>
				<button type="submit" class="btn" id="submitBtn">Start Pipeline</button>
			</form>
		</div>

		<div class="card progress-container" id="statusBox">
			<h3 style="text-align:center;">Analysis Progress</h3>
			
			<div class="steps-diagram" id="stepsDiagram">
				<div class="step-item" id="step-init">Input</div>
				<div class="step-item" id="step-qc">Quality Control</div>
				<div class="step-item" id="step-assembly">Assembly</div>
				<div class="step-item" id="step-annotation">Annotation</div>
				<div class="step-item" id="step-analysis">Analysis</div>
				<div class="step-item" id="step-report">Report</div>
			</div>
			<br>

			<div class="progress-bar">
				<div class="progress-fill" id="progressFill">0%</div>
			</div>
			<div class="step-text" id="statusText">Initializing...</div>
			
			<div id="resultArea" style="text-align:center; display:none; margin-top:20px;">
				<h4>Analysis Complete!</h4>
				<p>Your files are ready for download:</p>
				<div id="downloadLinks"></div>
			</div>
		</div>

		<script>
			document.getElementById('inputType').addEventListener('change', function(e) {
				document.getElementById('readsInput').style.display = e.target.value === 'reads' ? 'block' : 'none';
				document.getElementById('genomeInput').style.display = e.target.value === 'genome' ? 'block' : 'none';
			});

			function updateSteps(currentStep) {
				const steps = ['init', 'qc', 'assembly', 'annotation', 'analysis', 'report'];
				let found = false;
				steps.forEach(s => {
					const el = document.getElementById('step-' + s);
					if (s === currentStep) {
						el.className = 'step-item active';
						found = true;
					} else if (!found) {
						el.className = 'step-item completed';
					} else {
						el.className = 'step-item';
					}
				});
			}

			document.getElementById('uploadForm').addEventListener('submit', async function(e) {
				e.preventDefault();
				const btn = document.getElementById('submitBtn');
				const statusBox = document.getElementById('statusBox');
				const formData = new FormData(this);

				btn.disabled = true;
				statusBox.style.display = 'block';
				
				try {
					const uploadRes = await fetch('/upload', { method: 'POST', body: formData });
					const uploadData = await uploadRes.json();
					const jobID = uploadData.job_id;

					const poll = setInterval(async () => {
						const res = await fetch('/status?job_id=' + jobID);
						const status = await res.json();
						
						// Update Progress Bar
						document.getElementById('progressFill').style.width = status.progress + '%';
						document.getElementById('progressFill').innerText = status.progress + '%';
						document.getElementById('statusText').innerText = status.current_step;
						
						// Update Diagram
						let stepKey = 'init';
						if (status.current_step.includes('Quality')) stepKey = 'qc';
						else if (status.current_step.includes('Assembly')) stepKey = 'assembly';
						else if (status.current_step.includes('Annotation')) stepKey = 'annotation';
						else if (status.current_step.includes('Running Analysis')) stepKey = 'analysis';
						else if (status.current_step.includes('Report')) stepKey = 'report';
						updateSteps(stepKey);

						if (status.state === 'completed') {
							clearInterval(poll);
							updateSteps('report'); // Ensure all green
							document.getElementById('resultArea').style.display = 'block';
							
							// Fix Links: Ensure they point to the correct static path
							const baseUrl = '/download/' + jobID + '/output/';
							
							let linksHtml = '<a href="' + baseUrl + 'final_report.html" class="download-btn" target="_blank">View HTML Report</a>';
							linksHtml += '<a href="' + baseUrl + 'final_summary.json" class="download-btn" target="_blank">Summary JSON</a><br>';
							linksHtml += '<a href="' + baseUrl + 'contigs.fa" class="download-btn">Contigs (FASTA)</a>';
							linksHtml += '<a href="' + baseUrl + 'amr/amr_results.tsv" class="download-btn">AMR Results (TSV)</a>';
							
							document.getElementById('downloadLinks').innerHTML = linksHtml;
							btn.disabled = false;
						}
					}, 2000);
				} catch (err) {
					alert('Error: ' + err.message);
					btn.disabled = false;
				}
			});
		</script>
	</body>
	</html>
	`
	w.Write([]byte(html))
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" { http.Error(w, "POST only", 405); return }
	r.ParseMultipartForm(200 << 20)
	
	inputType := r.FormValue("type")
	jobID := fmt.Sprintf("job_%d", time.Now().Unix())

	localWorkDir, _ := filepath.Abs("workspace")
	jobDir := filepath.Join(localWorkDir, "jobs", jobID, "input")
	os.MkdirAll(jobDir, 0777)
	os.Chmod(filepath.Join(localWorkDir, "jobs", jobID), 0777)

	// Save Initial Status
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

// Helper to update status file
func updateJobStatus(jobID, state string, progress int, step string) {
	localWorkDir, _ := filepath.Abs("workspace")
	statusFile := filepath.Join(localWorkDir, "jobs", jobID, "status.json")
	
	status := JobStatus{State: state, Progress: progress, CurrentStep: step}
	bytes, _ := json.Marshal(status)
	os.WriteFile(statusFile, bytes, 0644)
	os.Chmod(statusFile, 0666) // Ensure readable
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

func handleBioJob(ctx context.Context, t *asynq.Task) error {
	var p JobPayload
	json.Unmarshal(t.Payload(), &p)
	
	// Helper to update status inside worker
	updateStatus := func(prog int, step string) {
		updateJobStatus(p.JobID, "running", prog, step)
	}

	workDir, _ := filepath.Abs("workspace")
	jobDir := filepath.Join(workDir, "jobs", p.JobID)
	outDir := filepath.Join(jobDir, "output")
	os.MkdirAll(outDir, 0777)
	os.Chmod(outDir, 0777)

	dockerOut := fmt.Sprintf("/workspace/jobs/%s/output", p.JobID)
	var contigs string

	// --- Step 1: QC & Assembly ---
	if p.InputType == "reads" {
		updateStatus(10, "Quality Control (FastQC)...")
		runDocker(IMG_FASTQC, "fastqc", filepath.Join("/workspace", p.FileR1), filepath.Join("/workspace", p.FileR2), "-o", filepath.Join(dockerOut, "fastqc"))
		
		updateStatus(30, "Genome Assembly (Shovill)...")
		runDocker(IMG_SHOVILL, "shovill", "--R1", filepath.Join("/workspace", p.FileR1), "--R2", filepath.Join("/workspace", p.FileR2), "--outdir", filepath.Join(dockerOut, "shovill"), "--force", "--cpus", "2", "--ram", "4")
		contigs = filepath.Join(dockerOut, "shovill", "contigs.fa")
	} else {
		updateStatus(10, "Processing Input Genome...")
		exec.Command("cp", filepath.Join(workDir, p.FileFasta), filepath.Join(outDir, "contigs.fa")).Run()
		contigs = filepath.Join(dockerOut, "contigs.fa")
	}

	// Verify Assembly
	// NOTE: We check local path for existence
	localContigs := filepath.Join(outDir, "contigs.fa")
	if p.InputType == "reads" { localContigs = filepath.Join(outDir, "shovill", "contigs.fa") }
	
	if _, err := os.Stat(localContigs); os.IsNotExist(err) {
		updateJobStatus(p.JobID, "error", 0, "Assembly Failed")
		return fmt.Errorf("assembly failed")
	}

	// --- Step 2: Annotation ---
	updateStatus(60, "Genome Annotation (Prokka)...")
	runDocker(IMG_PROKKA, "prokka", "--outdir", filepath.Join(dockerOut, "prokka"), "--force", "--prefix", "genome", contigs)

	// --- Step 3: Analysis ---
	updateStatus(80, "Running Analysis (AMR, Plasmid, QUAST)...")
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); runDocker(IMG_QUAST, "quast.py", contigs, "-o", filepath.Join(dockerOut, "quast")) }()
	go func() { defer wg.Done(); runDocker(IMG_AMR, "amrfinder", "-n", contigs, "-o", filepath.Join(dockerOut, "amr", "amr_results.tsv")) }()
	go func() { defer wg.Done(); runDocker(IMG_PLASMID, "plasmidfinder.py", "-i", contigs, "-o", filepath.Join(dockerOut, "plasmid")) }()
	wg.Wait()

	// --- Step 4: Report ---
	updateStatus(90, "Generating Final Report...")
	generateFinalReport(p, outDir)
	
	// Complete
	updateJobStatus(p.JobID, "completed", 100, "Analysis Complete")
	return nil
}

func runDocker(img string, args ...string) {
	host, _ := os.Getwd()
	if h := os.Getenv("HOST_WORKSPACE"); h != "" { host = h }
	cmdArgs := append([]string{"run", "--rm", "-v", host + ":/workspace", "-w", "/workspace", img}, args...)
	exec.Command("docker", cmdArgs...).Run()
}

func generateFinalReport(p JobPayload, dir string) {
	// 1. JSON Report
	r := FinalReport{JobID: p.JobID, Status: "completed", InputType: p.InputType}
	
	// Try Read QUAST Stats
	if f, err := os.Open(filepath.Join(dir, "quast", "transposed_report.tsv")); err == nil {
		s := bufio.NewScanner(f); s.Scan(); s.Scan() 
		parts := strings.Split(s.Text(), "\t")
		if len(parts) > 1 { r.Assembly.TotalLength = parts[1]; r.Assembly.N50 = parts[14]; r.Assembly.GC = parts[16]; r.Assembly.Contigs = parts[1] }
		f.Close()
	}
	
	jsonBytes, _ := json.MarshalIndent(r, "", "  ")
	os.WriteFile(filepath.Join(dir, "final_summary.json"), jsonBytes, 0644)

	// 2. HTML Report
	html := fmt.Sprintf(`
		<html><head><style>body{font-family:sans-serif;padding:30px;line-height:1.6} .box{border:1px solid #ddd;padding:20px;margin-bottom:20px;border-radius:8px}</style></head>
		<body>
			<h1>Analysis Report: %s</h1>
			<div class="box">
				<h3>Assembly Stats</h3>
				<ul><li>Length: %s bp</li><li>N50: %s</li><li>GC: %s%%</li><li>Contigs: %s</li></ul>
			</div>
			<div class="box">
				<h3>Output Files</h3>
				<ul>
					<li><a href="quast/report.html">Detailed Quality Report (QUAST)</a></li>
					<li><a href="contigs.fa">Assembled Contigs (FASTA)</a></li>
					<li><a href="prokka/genome.gff">Annotations (GFF)</a></li>
					<li><a href="amr/amr_results.tsv">AMR Resistance Genes (TSV)</a></li>
				</ul>
			</div>
			<p>Generated by BioFlow</p>
		</body></html>`, p.JobID, r.Assembly.TotalLength, r.Assembly.N50, r.Assembly.GC, r.Assembly.Contigs)
	
	os.WriteFile(filepath.Join(dir, "final_report.html"), []byte(html), 0644)
}