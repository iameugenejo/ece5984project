from airflow.models import Variable


class Field:
    JobTitle = Variable.get('TITLE_COLUMN', default_var='Job Title')
    JobTitleNormalized = 'Job Title Normalized'
    JobDescription = Variable.get('DESCRIPTION_COLUMN', default_var='Job Description')
    JobDescriptionTokens = 'Job Description Tokens'


class File:
    RawUncleanedFileName = 'Uncleaned_DS_jobs.csv'
    UncleanedFileName = RawUncleanedFileName + '.pkl'
    CleanedFileName = 'project-clean.pkl'
    TrainedFileNameFormat = 'project-trained-{}.pkl'
    ResultFileNameFormat = 'project-result-{}.pkl'


class RemoteFile:
    SourcePath = Variable.get('SOURCE_PATH', default_var='https://github.com/iameugenejo/ece5984project/raw/4cc67ea56ad4d5fdb7ce25849a31056edb347ca5/Uncleaned_DS_jobs.csv.zip')
    DataLakePath = Variable.get('DL_PATH', default_var='s3://ece5984-bucket-eugenejj/Project')
    DataWarehousePath = Variable.get('DW_PATH', default_var='s3://ece5984-bucket-eugenejj/Project')
