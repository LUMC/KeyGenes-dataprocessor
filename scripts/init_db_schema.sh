name=$(cat $3)
mysql -u $1 --password=$2 ${name} < $4
echo "success" > $5
