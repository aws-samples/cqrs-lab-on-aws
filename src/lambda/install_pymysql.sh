pip install -r requirements.txt --root .
cp -r usr/lib64/python3.6/dist-packages/* ./initdb
cp -r usr/lib64/python3.6/dist-packages/* ./query
cp -r usr/lib64/python3.6/dist-packages/* ./sink
rm -rf ./usr
