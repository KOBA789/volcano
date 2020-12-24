require "json"
require "mysql2"

db = Mysql2::Client.new(
  "host" => "127.0.0.1",
  "port" => "3306",
  "database" => ENV["MYSQL_DBNAME"] || "isucari",
  "username" => ENV["MYSQL_USER"] || "isucari",
  "password" => ENV["MYSQL_PASS"] || "isucari",
  "charset" => "utf8mb4",
  "database_timezone" => :local,
  "cast_booleans" => true,
  "reconnect" => true,
)
users = db.query("SELECT * FROM users")
File.open("data/users.json", "w") do |f|
  users.each do |user|
    f.puts(user.to_json)
  end
end

items = db.query("SELECT * FROM items")
File.open("data/items.json", "w") do |f|
  items.each do |item|
    f.puts(item.to_json)
  end
end

