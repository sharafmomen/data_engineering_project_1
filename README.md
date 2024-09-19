# Petroineos_TakeHomeAssessment

## Aim

The aim for this assessment is to create a data pipeline that can extract an excel file 

## Scraping

Although the title is along the lines of, "Supply and use of crude oil, natural gas liquids and feedstocks (ET 3.1 - quarterly)", reading the description and the actual link for the download shows that it's updated more frequently than expected. Here is the example download link:
"https://assets.publishing.service.gov.uk/media/66a76ff1ab418ab055592e8a/ET_3.1_JUL_24.xlsx"

Therefore, I have decided to read the month and year values towards the end of the link, which is nested deeply in the html of the provided link. 