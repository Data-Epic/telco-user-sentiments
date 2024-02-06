from jinja2 import Environment, FileSystemLoader
import yaml
import os

def dag_generator_sport():
    """
     Generates Airflow DAG files for sports from YAML configuration files.

    :return: None
    """
    file_dir = os.path.dirname(os.path.abspath(__file__))
    file_dir = os.path.join(file_dir, "..")
    env = Environment(loader=FileSystemLoader(file_dir))
    template = env.get_template("templates/dag_template.jinja2")
    for filename in os.listdir(os.path.join(file_dir, "inputs/sport")):
        print(filename)
        if filename.endswith('.yml'):
            with open(os.path.join(file_dir, "inputs/sport", filename), "r") as input_file:
                inputs = yaml.safe_load(input_file)
            output_dir = os.path.join(file_dir, "dags")
            os.makedirs(output_dir, exist_ok=True)
            output_file_path = os.path.join(output_dir, f"el_airflow_sport_{inputs['dag_id']}.py")
            with open(output_file_path, "w") as f:
                f.write(template.render(inputs))

dag_generator_sport()