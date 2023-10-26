### What have you done so far?
- [x] Define the scope of the project
- [x] Choose the dataset
  - https://www.kaggle.com/datasets/rashikrahmanpritom/data-science-job-posting-on-glassdoor
- [ ] Choose the pipeline
- [ ] Set-up the pipeline
- [x] Explore, assess, and transform the data, as needed

The scope of this project is to create a data pipeline that takes the raw web-scraped job posting data from Glassdoor, and build a text classification model that can predict the job title. This dataset provides several features such as salary range, company name, size, industry and many others, but we are mainly using the job title as the label data and the raw job description as the feature data to build the classifier.

For Exploratory Data Analysis, we have looked at the basic dataframe information and verified the missing, duplicate, and unique values. We also looked at the various job titles in the dataset to determine the strategy to clean and bucket the titles together. The title had varying version of seniority (Senior, Sr., Jr., Principal, etc.) in the title and same job title with specific industry (e.g. Data Scientist - Algorithms) we need to tackle. By grouping the different job titles, we identified following 9 groups of job titles:
  - data scientist
  - data engineer
  - senior data scientist
  - machine learning engineer
  - machine learning scientist
  - data analyst
  - data science manager
  - senior data analyst
  - senior data engineer

We decided to bucket all the positions in the management category by the following keywords:
  - manager
  - management
  - director
  - vp
  - president

We also decided to bucket the senior positions with the following keywords
  - senior
  - sr
  - experienced
  - ii
  - iii
  - staff
  - lead
  - principal

After bucketing the job titles, total of 172 unique job titles was reduced down to 71, and the number of job description matching the 9 groups is 583 from the total of 672 (86.7%) which is the majority of the dataset.

We also looked at the basic exploratory statistics on the Job Description field. 


### What pipeline, project, and dataset are you using?

* Pipeline: TBD
* Project: Predict Job title based on Job description
* Dataset: [Data Science Job Posting on Glassdoor](https://www.kaggle.com/datasets/rashikrahmanpritom/data-science-job-posting-on-glassdoor)

### Does your data need data cleaning or preprocessing? If so, what?
* Text Normalization
  * Case Normalization
  * Punctuation Removal
  * Stop word removal
  * Lemmatization
  
### Will you perform Exploratory Data Analysis? Which methods are you going to use?
* Inspect Panda DataFrame

### What information about data provenance have you listed? Answer the characteristic data provenance questions addressed in Module 5
TBD

### Are there any untested assumptions or other reasons that would prevent you from completing your project?
TBD

### What are your next steps?
TBD

### What is left to do?
- [ ] Choose your analysis model, as needed
- [ ] Run the pipeline
- [ ] Show the results
- [ ] Document the Project
