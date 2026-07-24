fn main() {
    tonic_build::compile_protos("proto/turbovec/v1/turbovec.proto")
        .expect("compile turbovec.v1 proto");
}
