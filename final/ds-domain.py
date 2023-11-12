class Field:
    JobTitle = 'Job Title'
    JobTitleNormalized = 'Job Title Normalized'
    JobDescription = 'Job Description'
    JobDescriptionTokens = 'Job Description Tokens'


class File:
    UncleanedFileName = 'Uncleaned_DS_jobs.csv'
    UncleanedFileNameZip = UncleanedFileName + '.zip'
    CleanedFileName = 'project-clean.pkl'
    TrainedFileNameFormat = 'project-trained-{}.pkl'
    ResultFileNameFormat = 'project-result-{}.pkl'


class RemoteFile:
    SourcePath = 'https://github.com/iameugenejo/ece5984project/raw/4cc67ea56ad4d5fdb7ce25849a31056edb347ca5/Uncleaned_DS_jobs.csv.zip'
    DataLakePath = 's3://ece5984-bucket-eugenejj/Project'
    DataWarehousePath = 's3://ece5984-bucket-eugenejj/Project'
