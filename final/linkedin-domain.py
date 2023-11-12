from airflow.models import Variable


class Field:
    JobTitle = Variable.get('TITLE_COLUMN', default_var='title')
    JobTitleNormalized = 'Job Title Normalized'
    JobDescription = Variable.get('DESCRIPTION_COLUMN', default_var='description')
    JobDescriptionTokens = 'Job Description Tokens'


class File:
    UncleanedFileName = 'job_postings.csv'
    UncleanedFileNameZip = UncleanedFileName + '.zip'
    CleanedFileName = 'project-clean.pkl'
    TrainedFileNameFormat = 'project-trained-{}.pkl'
    ResultFileNameFormat = 'project-result-{}.pkl'


class RemoteFile:
    SourcePath = Variable.get('SOURCE_PATH', default_var='https://github.com/iameugenejo/2023-linkedin-job-posting/raw/f4fa4265797ff1d4963cb9bcdb20dd3bd8cbdd97/job_postings.csv.zip')
    DataLakePath = Variable.get('DL_PATH', default_var='s3://ece5984-bucket-eugenejj/Project')
    DataWarehousePath = Variable.get('DW_PATH', default_var='s3://ece5984-bucket-eugenejj/Project')
