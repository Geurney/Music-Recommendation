ua:
	sbatch submit-ua.sh $(input)

au:
	sbatch submit-au.sh $(input)

toseq:
	sbatch submit-txt2seq.sh $(input)

cleanseq:
	rm -rf data/convertedOut

cleanout:
	rm -f out/*.out

cleanlog:
	rm -f *.out

cleanall: cleanseq cleanout cleanlog
