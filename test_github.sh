#!/bin/bash
set -e
echo "🧬 Downloading REAL Biological Test Data..."

mkdir -p workspace/input

if [ ! -f "workspace/input/R1.fastq.gz" ]; then
    wget -O workspace/input/R1.fq.gz "https://github.com/tseemann/shovill/raw/master/test/R1.fq.gz"
    wget -O workspace/input/R2.fq.gz "https://github.com/tseemann/shovill/raw/master/test/R2.fq.gz"
    mv workspace/input/R1.fq.gz workspace/input/R1.fastq.gz
    mv workspace/input/R2.fq.gz workspace/input/R2.fastq.gz
    echo "Reads downloaded (compressed)."
fi

if [ ! -f "workspace/input/genome.fa.gz" ]; then
    wget -O workspace/input/genome.fa "https://raw.githubusercontent.com/BenLangmead/bowtie2/master/example/reference/lambda_virus.fa"
    gzip -k workspace/input/genome.fa
    echo "Genome downloaded and compressed."
fi

chmod -R 777 workspace