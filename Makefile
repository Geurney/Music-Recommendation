ua:
	sbatch submit-ua.sh $(input)

au:
	sbatch submit-au.sh $(input)

co:
	sbatch submit-co.sh $(input)

toseq:
	sbatch submit-toseq.sh $(input)

cleanseq:
	rm -rf data/convertedOut

cleanout:
	rm -f out/*.out

cleanlog:
	rm -f *.out

cleanall: cleanseq cleanout cleanlog
