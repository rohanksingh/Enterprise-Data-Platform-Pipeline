from src.common.config_loader import load_config, load_sources

config = load_config()
sources = load_sources()

print("Database Host:", config["database"]["host"])
print("Trades Path:", sources["sources"]["trades"]["path"])