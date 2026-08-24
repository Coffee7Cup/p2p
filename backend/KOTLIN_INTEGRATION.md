# Kotlin Integration Guide for P2P Backend

This guide explains how to use the UniFFI-generated Rust backend from your Kotlin application. The backend exposes an interface to connect to Tor, host an onion service, send messages, and receive incoming updates.

## 1. Setup

### Generating the Bindings
The bindings are generated using `uniffi-bindgen`. A script or command should be run after building the Rust crate:
```bash
cargo build --release
cargo run --bin uniffi-bindgen generate target/release/libbackend.so --language kotlin --out-dir bindings
```
*Note: Replace `.so` with `.dylib` on macOS or `.dll` on Windows.*

### Importing into Kotlin
1. Copy the generated `uniffi/backend/backend.kt` file into your Kotlin project (e.g., `src/main/java/uniffi/backend/`).
2. Include the native library (`libbackend.so`, `libbackend.dylib`, or `backend.dll`) in your JNI library path (e.g., `app/src/main/jniLibs/` for Android or root directory for standard JVM apps).
3. Ensure you have JNA (Java Native Access) and Kotlin Coroutines as dependencies in your `build.gradle` / `pom.xml`:
```gradle
dependencies {
    implementation("net.java.dev.jna:jna:5.13.0@aar") // Or the standard JAR for JVM
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.1")
}
```

## 2. Usage Guide

### Initializing Tracing (Optional but Recommended)
Initialize the Rust logger to see debug/info output from Tor.
```kotlin
import uniffi.backend.*

fun main() {
    initTracing()
    // ...
}
```

### Implementing `MsgReceiver`
Kotlin needs to provide an implementation of `MsgReceiver` to listen to messages and status updates from the Rust backend.

```kotlin
class MyReceiver : MsgReceiver {
    override fun msgFromRust(msg: BackendMsg) {
        when (msg) {
            is BackendMsg.TorStatus -> {
                println("Tor status changed: ${msg.status}")
                // Update UI based on connecting/online/offline
            }
            is BackendMsg.ChatMsg -> {
                println("Received message: ${msg.text}")
                // Add message to UI
            }
            is BackendMsg.Error -> {
                println("Error received: ${msg.message}")
            }
        }
    }
}
```

### Initializing the Client
You must create a `Client` and pass the `MsgReceiver`. This requires coroutines.
Provide paths for the state and cache directories where Tor can store keys and consensus data.

```kotlin
import kotlinx.coroutines.*

suspend fun startApp() {
    val receiver = MyReceiver()
    val stateDir = "/path/to/app/data/tor_state"
    val cacheDir = "/path/to/app/data/tor_cache"
    
    // The client will bootstrap Tor asynchronously
    val client = createClient(stateDir, cacheDir, receiver)
    
    // Start the Onion Service (returns your onion address)
    val myOnionAddr = client.startService("my_nickname")
    println("Listening on: $myOnionAddr")
    
    // Connect to another peer's Onion Service
    client.connectToOnionAddress("otherpeeraddress.onion", 80)
    
    // Send a message
    client.sendMessage("otherpeeraddress.onion", "Hello from Kotlin!")
}
```

## 3. When Kotlin Needs This
Kotlin applications (e.g., Android Apps, Desktop Compose Apps) will need this integration when they want to:
- Communicate anonymously and securely over the Tor network without writing network protocols manually.
- Benefit from Rust's safety and performance while maintaining a native Kotlin UI experience.
- Provide a P2P chat or data-sharing feature that traverses NAT automatically using Tor Onion Services.
