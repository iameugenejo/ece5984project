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
    DataLakePath = 's3://ece5984-bucket-eugenejj/Project'
    DataWarehousePath = 's3://ece5984-bucket-eugenejj/Project'