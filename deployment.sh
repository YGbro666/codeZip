#!/bin/bash

# 定义URL和目录
URL="https://github.com/dssc-group/RayServicePrototype/archive/main.zip"
TARGET_DIR="./"
TMP_DIR=$(mktemp -d)

# 下载zip文件
wget $URL -O $TMP_DIR/downloaded_files.zip

# 解压缩zip文件
unzip $TMP_DIR/downloaded_files.zip -d $TMP_DIR

# 移动文件到当前目录
mv $TMP_DIR/RayServicePrototype-main/src/main/resources/static/* $TARGET_DIR

# 删除临时目录
rm -r $TMP_DIR

# 打包所有文件为code.zip
zip -r code.zip *

echo "文件已下载并打包成 code.zip"

