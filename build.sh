#!/bin/bash
binName="bin/mysqlsync"

GOOS=linux GOARCH=amd64 go build -o "$binName"_linux
GOOS=darwin GOARCH=amd64 go build -o "$binName"_macos
GOOS=freebsd GOARCH=amd64 go build -o "$binName"_freebsd
GOOS=linux GOARCH=arm go build -o "$binName"_arm
GOOS=windows GOARCH=amd64 go build -o "$binName"_win64.exe

echo "build working is done, see below binary files"

ls "$binName"*
