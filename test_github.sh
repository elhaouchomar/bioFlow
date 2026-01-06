#!/bin/bash
set -e
echo "Downloading REAL Biological Test Data..."

mkdir -p workspace/input

if [ ! -f "workspace/input/R1.fastq" ]; then
    wget -O workspace/input/R1.fq.gz "https://github.com/tseemann/shovill/raw/master/test/R1.fq.gz"
    wget -O workspace/input/R2.fq.gz "https://github.com/tseemann/shovill/raw/master/test/R2.fq.gz"
    gunzip workspace/input/*.gz
    mv workspace/input/R1.fq workspace/input/R1.fastq
    mv workspace/input/R2.fq workspace/input/R2.fastq
    echo "Reads downloaded."
fi

if [ ! -f "workspace/input/genome.fa" ]; then
    wget -O workspace/input/genome.fa "https://raw.githubusercontent.com/BenLangmead/bowtie2/master/example/reference/lambda_virus.fa"
    echo "Genome downloaded."
fi

chmod -R 777 workspace