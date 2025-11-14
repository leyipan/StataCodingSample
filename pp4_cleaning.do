********************************************************************************
* Section 1: Read files and initial data cleaning
********************************************************************************

* Read Excel files
import excel "LP_Project List_CreateDate.xlsx", firstrow clear
save projectlist_temp.dta, replace

import excel "PM names and ID.xlsx", firstrow clear
save pmid_temp.dta, replace

* Work with projectlist
use projectlist_temp.dta, clear

* Rename Column
rename ProjectManager PM_Name

* Save temporary file
save projectlist_temp.dta, replace

* Work with pmid
use pmid_temp.dta, clear

* Rename column
capture rename B ProjectManagerName

* Drop unnecessary columns (C and D)
capture drop C D

* Drop first row (row 1) and rows 49-59
drop in 1
drop in 49/59

* Rename column
rename ProjectManagerName PM_Name

* Remove "Project Manager Name: " prefix
replace PM_Name = subinstr(PM_Name, "Project Manager Name: ", "", .)

* Save cleaned pmid
save pmid_temp.dta, replace

* Merge datasets
use projectlist_temp.dta, clear
merge m:1 PM_Name using pmid_temp.dta, keep(master match)
drop _merge

* Rename Column
rename ProjectManagers pmid

* Transform the project column - remove all periods
replace Project = subinstr(Project, ".", "", .)

* Rename columns
rename Project projectno
rename CreateDate create_date

* Save merged dataset
save pl_merged.dta, replace

* Clean up temporary files
erase projectlist_temp.dta
erase pmid_temp.dta

********************************************************************************
* Section 2: Import overall dataframe and merge with project data
********************************************************************************

* Import overall CSV file
import delimited "LP_v4 Refined OveralL DB.csv", clear
save overall_temp.dta, replace

* Load pl_merged from previous section
use pl_merged.dta, clear

* Step 1: Keep only relevant columns
keep projectno create_date
* Check for duplicates and handle them
* Keep only the first occurrence of each projectno
bysort projectno (create_date): keep if _n == 1
save pl_selected_temp.dta, replace

* Step 2: Load overall and prepare for merge
use overall_temp.dta, clear

* Ensure projectNo is string format
tostring projectno, replace

* Left join pl_selected to overall on projectNo
merge m:1 projectno using pl_selected_temp.dta, keep(master match)
drop _merge

* Form the month, day, and year columns
* Check if create_date is already numeric or string
capture confirm numeric variable create_date

if _rc == 0 {
    * create_date is already numeric (Stata date format)
    gen project_Start_Month = month(create_date)
    gen project_Start_Day = day(create_date)
    gen project_Start_Year = year(create_date)
}
else {
    * create_date is string, need to convert
    gen create_date_num = date(create_date, "DMY")
    gen project_Start_Month = month(create_date_num)
    gen project_Start_Day = day(create_date_num)
    gen project_Start_Year = year(create_date_num)
    drop create_date_num
}

* Save the joined dataset
save joined_df.dta, replace

* Clean up temporary files
erase overall_temp.dta
erase pl_selected_temp.dta

********************************************************************************
* Section 3: Interrupted Time Series Analysis
********************************************************************************

* Import interrupted time series
import delimited "LP_Interrupted Time Series Database.csv", clear

* Keep only necessary columns
keep year month time newbldg fivetotalbilled fivetotalspent threetotalbilled ///
     threetotalspent mtotalbilled mtotalspent

* Save time_series
save time_series_temp.dta, replace

* Create expanded dataset with 48 repetitions of each row
* First, get the number of observations
local n_obs = _N

* Expand each observation 48 times
expand 48

* Create pmid variable (1 to 48 for each original observation)
bysort year month time: gen pmid = _n

* Save expanded time series
save time_series_expanded.dta, replace

* Work with joined_df to calculate totals
use joined_df.dta, clear

* Keep only relevant columns
keep project_Start_Year project_Start_Month pmid spent billed

* Calculate sum of Spent and Billed by year, month, and pmid
collapse (sum) total_spent=spent total_billed=billed, ///
    by(project_Start_Year project_Start_Month pmid)

* Create index column
gen index = string(project_Start_Year) + "_" + string(project_Start_Month) + "_" + string(pmid)

* Save summed data
save summed_df.dta, replace

* Create index in time_series_expanded
use time_series_expanded.dta, clear
gen index = string(year) + "_" + string(month) + "_" + string(pmid)

* Left join using index
merge m:1 index using summed_df.dta, keep(master match)
drop _merge

* Delete unnecessary columns
drop project_Start_Year project_Start_Month index

* Export joined_time_series to Excel
export excel using "monthly_by_pm.xlsx", firstrow(variables) replace

* Save for further processing
save joined_time_series.dta, replace

* Calculate sums by year and month
* Total spent sum
use joined_time_series.dta, clear
collapse (sum) total_spent_sum=total_spent, by(year month)
save total_spent_result.dta, replace

* Total billed sum
use joined_time_series.dta, clear
collapse (sum) total_billed_sum=total_billed, by(year month)
save total_billed_result.dta, replace

* Merge back to original time_series
use time_series_temp.dta, clear
merge 1:1 year month using total_spent_result.dta, keep(master match)
drop _merge
merge 1:1 year month using total_billed_result.dta, keep(master match)
drop _merge

* Export final time series to Excel
export excel using "Interrupted Time Series with Projects.xlsx", firstrow(variables) replace

* Clean up temporary files
erase time_series_temp.dta
erase time_series_expanded.dta
erase summed_df.dta
erase total_spent_result.dta
erase total_billed_result.dta
