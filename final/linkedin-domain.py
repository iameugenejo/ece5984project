class Field:
    JobTitle = 'title'
    JobTitleNormalized = 'Job Title Normalized'
    JobDescription = 'description'
    JobDescriptionTokens = 'Job Description Tokens'


class File:
    UncleanedFileName = 'job_postings.csv'
    UncleanedFileNameZip = UncleanedFileName + '.zip'
    CleanedFileName = 'project-clean.pkl'
    TrainedFileNameFormat = 'project-trained-{}.pkl'
    ResultFileNameFormat = 'project-result-{}.pkl'


class RemoteFile:
    SourcePath = 'https://github.com/iameugenejo/2023-linkedin-job-posting/raw/f4fa4265797ff1d4963cb9bcdb20dd3bd8cbdd97/job_postings.csv.zip'
    DataLakePath = 's3://ece5984-bucket-eugenejj/Project'
    DataWarehousePath = 's3://ece5984-bucket-eugenejj/Project'
