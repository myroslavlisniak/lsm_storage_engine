pub enum Command {
    Get(Vec<u8>),
    Insert(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Update(Vec<u8>, Vec<u8>),
    NotSupported,
}

impl Command {
    pub fn parse(input: &str) -> Command {
        let args: Vec<&str> = input.split_whitespace().collect();
        if args.is_empty() {
            Command::NotSupported
        } else {
            match args[0] {
                "get" => Command::Get(String::from(args[1]).into_bytes()),
                "insert" => Command::Insert(String::from(args[1]).into_bytes(), String::from(args[2]).into_bytes()),
                "update" => Command::Update(String::from(args[1]).into_bytes(), String::from(args[2]).into_bytes()),
                "delete" => Command::Delete(String::from(args[1]).into_bytes()),
                _ => Command::NotSupported
            }
        }
    }
}
