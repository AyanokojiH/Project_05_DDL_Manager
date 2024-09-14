import yaml

# 配置信息
config = {
    'host': 'localhost',
    'user': 'root',
    'password': '', #填密码
    'database': 'ddl_manager'
}

# 将配置信息写入 YAML 文件
def write_config_to_yaml(config, filename="config.yaml"):
    with open(filename, 'w') as file:
        yaml.dump(config, file, default_flow_style=False)

# 调用函数
write_config_to_yaml(config)