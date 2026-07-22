# Peer to Peer chat app

## Generate FFI using the following in backend folder

```bash

cargo build --release #this will create a libbackend.so file in target/release/
cargo run --bin uniffi-bindgen --library target/release/libbackend.so --language kotlin --out-dir out # This will create outpuy (.kt) file in out(backend/out) dir

```
