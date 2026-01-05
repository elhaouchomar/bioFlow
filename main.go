package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/hibiken/asynq"
)

const (
	IMG_FASTQC  = "staphb/fastqc:latest"
	IMG_SHOVILL = "staphb/shovill:latest"
	IMG_QUAST   = "staphb/quast:latest"
	IMG_PROKKA  = "staphb/prokka:latest"
	IMG_AMR     = "staphb/ncbi-amrfinderplus:latest"
	IMG_PLASMID = "staphb/plasmidfinder:latest"

	TypeBioJob = "bio:pipeline_job"
)

type JobPayload struct {
	JobID     string `json:"job_id"`
	InputType string `json:"input_type"`
	FileR1    string `json:"file_r1,omitempty"`
	FileR2    string `json:"file_r2,omitempty"`
	FileFasta string `json:"file_fasta,omitempty"`
}

func main() {
	mode := flag.String("mode", "client", "Mode: 'worker' or 'client'")
	inputType := flag.String("type", "", "Input type for client: 'reads' or 'genome'")
	flag.Parse()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	if *mode == "worker" {
		runWorker(redisAddr)
	} else {
		runClient(redisAddr, *inputType)
	}
}

// ---------------------------------------------------------
// 1. Worker Logic
// ---------------------------------------------------------
func runWorker(redisAddr string) {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddr},
		asynq.Config{Concurrency: 2},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(TypeBioJob, handleBioJob)

	fmt.Println(" [*] Worker started. Waiting for tasks...")
	if err := srv.Run(mux); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}

func handleBioJob(ctx context.Context, t *asynq.Task) error {
	var p JobPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json unmarshal failed: %v", err)
	}

	fmt.Printf("\n🚀 Starting Job: %s | Type: %s\n", p.JobID, p.InputType)

	localWorkDir, _ := filepath.Abs("workspace")
	localJobDir := filepath.Join(localWorkDir, "jobs", p.JobID)
	localOutputDir := filepath.Join(localJobDir, "output")
	
	os.MkdirAll(localOutputDir, 0755)

	os.Chmod(localJobDir, 0777)
	os.Chmod(localOutputDir, 0777)

	dockerJobOut := fmt.Sprintf("/workspace/jobs/%s/output", p.JobID)

	var finalContigsDocker string
	var finalContigsLocal string

	switch p.InputType {
	case "reads":
		fmt.Println(" [1] Processing Raw Reads (Assembly)...")
		
		dFileR1 := filepath.Join("/workspace", p.FileR1)
		dFileR2 := filepath.Join("/workspace", p.FileR2)

		// 1. FastQC
		fastqcOutLocal := filepath.Join(localOutputDir, "fastqc")
		os.MkdirAll(fastqcOutLocal, 0777)
		os.Chmod(fastqcOutLocal, 0777)
		
		fastqcOutDocker := filepath.Join(dockerJobOut, "fastqc")
		runDockerTool(IMG_FASTQC, "fastqc", dFileR1, dFileR2, "-o", fastqcOutDocker)

		// 2. Shovill
		shovillOutLocal := filepath.Join(localOutputDir, "shovill")
		
		shovillOutDocker := filepath.Join(dockerJobOut, "shovill")

		err := runDockerTool(IMG_SHOVILL, "shovill", 
			"--R1", dFileR1, 
			"--R2", dFileR2, 
			"--outdir", shovillOutDocker, 
			"--force", 
			"--cpus", "2", 
			"--ram", "4") 
		if err != nil {
			return fmt.Errorf("Assembly (Shovill) failed: %v", err)
		}
		
		finalContigsDocker = filepath.Join(shovillOutDocker, "contigs.fa")
		finalContigsLocal = filepath.Join(shovillOutLocal, "contigs.fa")

	case "genome":
		fmt.Println(" [1] Using Input Genome...")
		src := filepath.Join(localWorkDir, p.FileFasta)
		dst := filepath.Join(localOutputDir, "contigs.fa")
		
		exec.Command("cp", src, dst).Run()
		os.Chmod(dst, 0666) 
		
		finalContigsDocker = fmt.Sprintf("/workspace/jobs/%s/output/contigs.fa", p.JobID)
		finalContigsLocal = dst

	default:
		return fmt.Errorf("invalid type")
	}

	if _, err := os.Stat(finalContigsLocal); os.IsNotExist(err) {
		logFile := filepath.Join(localOutputDir, "shovill", "shovill.log")
		if logs, errRead := os.ReadFile(logFile); errRead == nil {
			fmt.Printf("❌ Shovill Log:\n%s\n", string(logs))
		}
		return fmt.Errorf("❌ Error: Contigs file missing at %s. (Assembly failed)", finalContigsLocal)
	}

	fmt.Println(" [2] Running Analysis Tools...")

	dirs := []string{"prokka", "quast", "amr", "plasmid"}
	for _, d := range dirs {
		p := filepath.Join(localOutputDir, d)
		os.MkdirAll(p, 0777)
		os.Chmod(p, 0777)
	}

	var wg sync.WaitGroup
	
	// Prokka
	wg.Add(1)
	go func() {
		defer wg.Done()
		out := filepath.Join(dockerJobOut, "prokka")
		runDockerTool(IMG_PROKKA, "prokka", "--outdir", out, "--force", "--prefix", "genome", finalContigsDocker)
	}()

	// QUAST
	wg.Add(1)
	go func() {
		defer wg.Done()
		out := filepath.Join(dockerJobOut, "quast")
		runDockerTool(IMG_QUAST, "quast.py", finalContigsDocker, "-o", out)
	}()

	// AMRFinder
	wg.Add(1)
	go func() {
		defer wg.Done()
		out := filepath.Join(dockerJobOut, "amr_results.tsv")
		runDockerTool(IMG_AMR, "amrfinder", "-n", finalContigsDocker, "-o", out)
	}()

	// PlasmidFinder
	wg.Add(1)
	go func() {
		defer wg.Done()
		out := filepath.Join(dockerJobOut, "plasmid")
		runDockerTool(IMG_PLASMID, "plasmidfinder.py", "-i", finalContigsDocker, "-o", out)
	}()

	wg.Wait()
	fmt.Println("✅ Job Completed Successfully!")
	return nil
}

// ---------------------------------------------------------
// Helper: Run Docker Tool
// ---------------------------------------------------------
func runDockerTool(image string, args ...string) error {
	hostWorkspace := os.Getenv("HOST_WORKSPACE")
	if hostWorkspace == "" {
		hostWorkspace, _ = os.Getwd()
	}

	dockerArgs := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:/workspace", hostWorkspace),
		"-w", "/workspace",
		image,
	}
	dockerArgs = append(dockerArgs, args...)

	cmd := exec.Command("docker", dockerArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		logLen := len(output)
		start := 0
		if logLen > 500 { start = logLen - 500 }
		return fmt.Errorf("CMD Failed: %v\nLOGS (Tail): ...%s", err, output[start:])
	}
	return nil
}

// ---------------------------------------------------------
// 2. Client Logic
// ---------------------------------------------------------
func runClient(redisAddr, inputType string) {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer client.Close()

	var p JobPayload
	jobID := fmt.Sprintf("job_%d", time.Now().Unix())

	if inputType == "reads" {
		p = JobPayload{
			JobID:     jobID,
			InputType: "reads",
			FileR1:    "input/R1.fastq", 
			FileR2:    "input/R2.fastq",
		}
	} else {
		p = JobPayload{
			JobID:     jobID,
			InputType: "genome",
			FileFasta: "input/genome.fa",
		}
	}

	data, _ := json.Marshal(p)
	task := asynq.NewTask(TypeBioJob, data)
	client.Enqueue(task)
	fmt.Printf(" [x] Task Enqueued: %s (ID: %s)\n", inputType, jobID)
}