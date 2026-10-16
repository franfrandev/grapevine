build_app:
	cargo build -p grapevine

build_web:
	wasm-pack build web --target no-modules

keys:
	openssl ecparam -name secp256k1 -genkey -noout -out secp256k1.pem; \
	openssl ec -in secp256k1.pem -pubout -out secp256k1.pub; \
	openssl ec -in secp256k1.pem -outform DER -out secp256k1.der