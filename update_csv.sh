cd ~/GitHub/police-logs/;

git remote add upstream https://github.com/mit/police-logs.git
git fetch upstream
git checkout master
git merge upstream/master

python update_csv.py
git add full_logs.csv processed_logs.txt
git commit -m 'updating .csv'
git push
