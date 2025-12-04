fn main() {
    let flow = example_s2_site::flow();
    let json = serde_json::to_string_pretty(&flow).expect("serialize flow");
    println!("{}", json);
}
