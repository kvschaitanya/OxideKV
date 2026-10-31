fn main() -> Result<(), std::error::Error> {
    tonic_build::compile_protos("proto/raft.proto")?;
    Ok(())
}
