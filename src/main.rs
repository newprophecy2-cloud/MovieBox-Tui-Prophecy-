#[cfg(not(target_os = "android"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    moviebox_tui::logging::init();
    moviebox_tui::api::run().await
}
