//! Build script for boyodb-node native module
//!
//! This sets up the correct linker flags for NAPI on different platforms.

fn main() {
    // On macOS, we need to allow undefined symbols because NAPI functions
    // are provided at runtime by Node.js, not at link time.
    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }

    // On Linux, symbols are resolved at runtime by default for shared libraries
    // No special flags needed

    // Tell cargo to re-run this script if it changes
    println!("cargo:rerun-if-changed=build.rs");
}
