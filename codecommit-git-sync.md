Create a id_rsa.pub file to use
---------------------
ssh-keygen -t rsa -C "codecommit"
cat .ssh/id_rsa.pub

Copy this and create IAM user key

git clone https://github.com/CSSEGISandData/COVID-19.git covid19data
git remote add sync ssh://KEY_FROM_SSH_IAM_USER@git-codecommit.therepo
git push sync

crontab -e
> 45 2 * * * cd ~/covid19data && git pull && git push sync
