import yaml

def load_config(path="configs/config.yaml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)
    
def load_sources(path="configs/sources.yaml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)
    

