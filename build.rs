fn main() {
    // Emit the file descriptor set alongside the generated code so the server
    // can answer gRPC reflection queries (grpcurl, language REPLs, tooling).
    let descriptor_set =
        std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("turbovec_v1.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_set)
        .compile_protos(
            &["proto/turbovec/v1/turbovec.proto"],
            &["proto"],
        )
        .expect("compile turbovec.v1 proto");
}
