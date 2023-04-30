import os
import apache_beam as beam
from dotenv import load_dotenv
from apache_beam.options.pipeline_options import _BeamArgumentParser, PipelineOptions
from apache_beam.io.mongodbio import WriteToMongoDB
from methods import *

load_dotenv(".env")

class JiraPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser: _BeamArgumentParser) -> None:
        parser.add_argument(
            "--apiurl",
            default=os.getenv("URL"),
            help="jira url"   
        )
        parser.add_argument(
            "--jiratoken",
            default=os.getenv("TOKEN"),
            help="jira api token"   
        )
        parser.add_argument(
            "--mongouri",
            default=os.getenv("MONGO_URI"),
            help="mongo database uri"   
        )
        parser.add_argument(
            "--username",
            default=os.getenv("MONGO_USERNAME"),
            help="mongo username"   
        )
        parser.add_argument(
            "--password",
            default=os.getenv("MONGO_PASSWORD"),
            help="mongo password"   
        )
        parser.add_argument(
            "--collection",
            default=os.getenv("COLLECTION_NANE"),
            help="collection name"   
        )

options = JiraPipelineOptions()

with beam.Pipeline(options=options) as pipeline:
    read_from_api = (
        pipeline
        | "Inicia pipeline" >> beam.Create([{
                                            "url": options.apiurl,
                                            "token": options.jiratoken,
                                            }])
        | "Read data from API" >> beam.ParDo(get_jira)
    )
    
    p_raw = (
        read_from_api
         | "Write raw data" >> WriteToMongoDB(uri=options.mongouri,
                                            db=options.collection,
                                            coll='raw_data',
                                            batch_size=10)        
    )
    
    p_task = (
        read_from_api
        | "Transform project data" >> beam.Map(create_project)
        | "Write project data" >> WriteToMongoDB(uri=options.mongouri,
                                                db=options.collection,
                                                coll='projetos',
                                                batch_size=10)
    )
    
    p_task = (
        read_from_api
        | "Transform task data" >> beam.Map(create_task)
        | "Write task data" >> WriteToMongoDB(uri=options.mongouri,
                                            db=options.collection,
                                            coll='tarefas',
                                            batch_size=10)
    )
    
    p_user = (
        read_from_api
        | "Transform user data" >> beam.Map(create_user)
        | "Write user data" >> WriteToMongoDB(uri=options.mongouri,
                                            db=options.collection,
                                            coll='usuarios',
                                            batch_size=10)
    )
    