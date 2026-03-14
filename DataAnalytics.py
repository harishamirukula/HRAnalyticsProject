import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/HR_db")
departments = pd.read_csv("HRAnalyticsProject/departments.csv")
#print(departments.head())
employees = pd.read_csv("HRAnalyticsProject/employees.csv")
#print(employees.head())
performance = pd.read_csv("HRAnalyticsProject/performance.csv")
#print(performance.head())
promotions = pd.read_csv("HRAnalyticsProject/promotions.csv")
#print(promotions.head())


#employees.columns = employees.columns.str.strip().str.lower().str.replace(" ", "_")
#performance.columns = performance.columns.str.strip().str.lower().str.replace(" ", "_")       
#departments.columns = departments.columns.str.strip().str.lower().str.replace(" ", "_")
#promotions.columns = promotions.columns.str.strip().str.lower().str.replace(" ", "_") 

employees["hire_date"] = pd.to_datetime(employees["hire_date"])
performance["review_date"] = pd.to_datetime(performance["review_date"]) 
promotions["promotion_date"] = pd.to_datetime(promotions["promotion_date"])
today = pd.Timestamp.today()
#employees["end_date"] = employees["termination_date"].fillna(today)
employees["tenure"] = ((today - employees["hire_date"]).dt.days / 365).round(2)
employees["termination_date"] = pd.to_datetime(employees["termination_date"], errors="coerce")
employees["is_active"] = employees["termination_date"].isna().astype(int)
employees["full_name"] = employees["first_name"] + " " + employees["last_name"]
employees["total_compensation"] = employees["salary"] + employees["bonus"]
#employees["age"] = (pd.to_datetime("2026-03-10") - employees["hire_date"]).dt.days / 365        
#employees["salary"] = employees["salary"].str.replace("$", "").astype(float)
#performance["performance_score"] = pd.to_numeric(performance["performance_score"], errors="coerce") 
employees = employees.merge(departments, on="department_id", how="left")
employee_performance = employees.merge(performance, on="employee_id", how="left")   
employee_performance["year"] = employee_performance["review_date"].dt.year
employee_performance["month"] = employee_performance["review_date"].dt.month
employee_performance["day"] = employee_performance["review_date"].dt.day
#employee_performance["performance_category"] = pd.cut(employee_performance["performance_score"], bins=[0, 2, 4, 5], labels=["Low", "Medium", "High"])
employee_performance["is_promoted"] = employee_performance["employee_id"].isin(promotions["employee_id"]).astype(int)       
#employee_performance.to_csv("HRAnalyticsProject/employee_performance.csv", index=False) 
'''loyee_performance.to_sql("employee_performance", engine, if_exists="replace", index=False)
employees.to_sql("employees", engine,if_exists="replace",index=False,chunksize=100,method="multi")
departments.to_sql("departments", engine, if_exists="replace", index=False)
performance.to_sql("performance", engine, if_exists="replace", index=False)
promotions.to_sql("promotions", engine, if_exists="replace", index=False)          
print(employees.head())
print(employees.info())'''
#print(departments.head())
#print(performance.head())
#print(promotions.head())    
#***3/12/2026***
employees["hire_yr"] = pd.to_datetime(employees["hire_date"]).dt.year
employees["hire_mo"] = pd.to_datetime(employees["hire_date"]).dt.month
print(employees["hire_yr"], employees["hire_mo"])

top_5paid = employees.sort_values("salary", ascending=True).iloc[3:4]
print(top_5paid[["first_name","last_name","age", "salary"]])

employees["salary_rank"] = employees.groupby("department_id")["salary"].rank(ascending=False, method="dense")
print(employees[["first_name", "last_name","department_id", "salary", "salary_rank"]].sort_values(["department_id", "salary_rank"]) )
#print(employees.info())

employees["sal_category"] = pd.cut(employees["salary"], bins=[0, 50000, 100000, 150000, 200000], labels=["Low", "Medium", "High", "Very High"])
employees["age_cat"] = pd.cut(employees["age"], bins=[0, 30, 40, 50, 60], labels=["Young", "Mid-age", "Senior", "Elderly"])
    #rint(employees[["first_name", "last_name", "salary", "sal_category", "age_cat"]].head())

employees["is_active"] = employees["termination_date"].isna().astype(int)

attrition = employees.groupby("department_id").agg(
    total_employees = ("employee_id", "count"),
    avg_salary = ("salary", "mean"),
    avg_age = ("age", "mean"),
    active_employees = ("is_active", "sum")
)
attrition["attrition_rate_percent"] = (1- (attrition["active_employees"] / attrition["total_employees"]))*100
#   print(attrition.reset_index())

employees["dept_avg_salary"] = employees.groupby("department_id")["salary"].transform("mean")
#rint(employees[["first_name", "last_name", "department_id", "salary", "dept_avg_salary"]].head())

#mployees = employees.merge(attrition[["avg_salary"]].rename(columns={"avg_salary": "dept_avg_salary"}), on="department_id", how="left")

#mployees["avg_sal"] = employees.groupby("department_id")["salary"].transform("mean")
top_5paid = employees.sort_values("salary", ascending=False).iloc[:5]
#rint(top_5paid[["first_name", "last_name", "department_id", "salary", "avg_sal"]])
#rint(employees.info())
employee_performance.to_sql("employee_performance", engine, if_exists="replace", index=False)
employees.to_sql("employees", engine,if_exists="replace",index=False,chunksize=100,method="multi")
departments.to_sql("departments", engine, if_exists="replace", index=False)
performance.to_sql("performance", engine, if_exists="replace", index=False)
promotions.to_sql("promotions", engine, if_exists="replace", index=False)