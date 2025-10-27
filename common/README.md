# Compilation

You need to match the version of protoc with the version in the pom.
Protoc is fickly, so make sure you have the exact version - this is difficult using homebrew.

On a mac, you download using this:

```
curl -OL https://github.com/google/protobuf/releases/download/v3.6.1/protoc-3.6.1-osx-x86_64.zip
```

Once downloaded, unpack it to a sensible place and then symlink using e.g.:

```
sudo ln -s ~/dev/protoc-3.6.1-osx-x86_64/bin/protoc /usr/local/bin/protoc_3.6.1
```

Check that it works:

```
tsj442@1027603 maps % /usr/local/bin/protoc_3.6.1 --version
libprotoc 3.6.1
```
