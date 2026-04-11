use std::net::IpAddr;
 
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // IPv6 address
    let local_addr: IpAddr = "2001:bc8:1640:4f72::1".parse()?;
    
    let client = reqwest::Client::builder()
        .local_address(local_addr)
        .build()?;
    
    let mut cpt = 0;
    loop {
        let response = client.get("https://backend.wplace.live/files/s0/tiles/1040/704.png").send().await?;
        if !response.status().is_success() {
            println!("{}", response.status().as_str());
            break;
        }
        cpt += 1;
    }
    println!("calls={}", cpt);
    Ok(())
}
 
