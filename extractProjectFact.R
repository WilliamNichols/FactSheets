#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Extract the project fact sheet and project process fidelity sheet
# 2014/7/17
# Yasutaka Shirai
# Update: 2015/2/21
# Add the function to select projects for extracting fact sheet
# Yasutaka Shirai

extractProjectFact <- function(con, currentDirectory)
{

# Get necessary data records from SEMPRE
#tab_project_info<-dbGetQuery(con, "select project_key,quote(project_name) as project_name from project")
tab_project_info<-dbGetQuery(con, "select project.project_key,quote(project_name) as project_name, parent_project_key, project_pattern from project left join project_layer on project.project_key = project_layer.project_key")
tab_organization_info<-dbGetQuery(con, "select project_key,org_mapping.organization_key, quote(organization_name) as organization_name from organization left join org_mapping on organization.organization_key = org_mapping.organization_key")
tab_process_info<-dbGetQuery(con, "select distinct project_key,phase.process_key,quote(process_name) as process_name from plan_item_hist left join phase on plan_item_hist.phase_key = phase.phase_key left join process on phase.process_key = process.process_key where phase.process_key is not null")
tab_teams_info<-dbGetQuery(con, "select distinct project_key, data_block.team_key, quote(team_name) as team_name, person_key from task_status_fact_hist left join plan_item_hist on task_status_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join data_block on task_status_fact_hist.data_block_key = data_block.data_block_key left join team on data_block.team_key = team.team_key");
tab_duration_info<-dbGetQuery(con, "select min(time_log_start_date) as start_date, DATE_FORMAT(min(time_log_start_date),'%Y%u') as start_week, max(time_log_end_date) as end_date, DATE_FORMAT(max(time_log_start_date),'%Y%u') as end_week, (DATE_FORMAT(max(time_log_start_date),'%Y%u')-DATE_FORMAT(min(time_log_start_date),'%Y%u')) as actual_weeks,project_key from time_log_fact_hist join plan_item_hist on time_log_fact_hist.plan_item_key = plan_item_hist.plan_item_key where time_log_fact_key != 23000 group by project_key")
#tab_time_info<-dbGetQuery(con,"select project_key,phase_short_name,phase.phase_key,phase_ordinal,min(task_actual_start_date_key) as task_begin_date, max(task_actual_complete_date_key) as task_end_date, sum(task_actual_time_minutes) as sum_actual_time,sum(task_plan_time_minutes) as sum_plan_time,phase_type from task_status_fact_hist left join plan_item_hist on task_status_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on plan_item_hist.phase_key = phase.phase_key left join phase_order on phase.phase_key = phase_order.phase_key where phase.phase_key is not null group by project_key,phase_short_name order by project_key,phase_ordinal,phase.phase_key")
tab_time_info<-dbGetQuery(con,"select project_key,phase_base.phase_short_name,phase_base.phase_base_key,phase_base.phase_ordinal,min(task_actual_start_date_key) as task_begin_date, max(task_actual_complete_date_key) as task_end_date, sum(task_actual_time_minutes) as sum_actual_time,sum(task_plan_time_minutes) as sum_plan_time,phase_type from task_status_fact_hist left join plan_item_hist on task_status_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on plan_item_hist.phase_key = phase.phase_key left join phase_mapping on phase.phase_key = phase_mapping.phase_key left join phase_base on phase_mapping.phase_base_key = phase_base.phase_base_key where phase_base.phase_base_key is not null group by project_key,phase_short_name order by project_key,phase_base.phase_ordinal,phase_base.phase_base_key")
#tab_time_log_info<-dbGetQuery(con, "SELECT time_log_fact_key, project_key, time_log_delta_minutes, time_log_start_date, time_log_end_date, DATE_FORMAT(time_log_start_date, '%Y-%m-%d') as start_day, DATE_FORMAT(time_log_end_date, '%Y-%m-%d') as end_day, plan_item_hist.phase_key, phase_short_name FROM time_log_fact_hist left join plan_item_hist on time_log_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on plan_item_hist.phase_key = phase.phase_key where time_log_start_date > '1900-01-01 00:00:00' and time_log_end_date > '1900-01-01 00:00:00'")
tab_time_log_info<-dbGetQuery(con, "SELECT time_log_fact_key, project_key, time_log_delta_minutes, time_log_start_date, time_log_end_date, DATE_FORMAT(time_log_start_date, '%Y-%m-%d') as start_day, DATE_FORMAT(time_log_end_date, '%Y-%m-%d') as end_day, phase_base.phase_base_key, phase_base.phase_short_name FROM time_log_fact_hist left join plan_item_hist on time_log_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on plan_item_hist.phase_key = phase.phase_key left join phase_mapping on phase.phase_key = phase_mapping.phase_key left join phase_base on phase_mapping.phase_base_key = phase_base.phase_base_key where time_log_start_date > '1900-01-01 00:00:00' and time_log_end_date > '1900-01-01 00:00:00'")
#tab_task_completion_info<-dbGetQuery(con, "SELECT task_date_fact_key, project_key, task_date_key, DATE_FORMAT(task_date_key, '%Y-%m-%d') as task_completion_date, measurement_type_key, phase_short_name, wbs_element_key FROM task_date_fact_hist left join plan_item_hist on task_date_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on plan_item_hist.phase_key = phase.phase_key")
tab_task_completion_info<-dbGetQuery(con, "SELECT task_date_fact_key, project_key, task_date_key, DATE_FORMAT(task_date_key, '%Y-%m-%d') as task_completion_date, measurement_type_key, phase_base.phase_short_name, wbs_element_key FROM task_date_fact_hist left join plan_item_hist on task_date_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on plan_item_hist.phase_key = phase.phase_key left join phase_mapping on phase.phase_key = phase_mapping.phase_key left join phase_base on phase_mapping.phase_base_key = phase_base.phase_base_key")
#tab_ev_info<-dbGetQuery(con, "select project_key,plan_item_hist.phase_key,phase_short_name,phase_type,wbs_element_key,task_actual_time_minutes,task_plan_time_minutes,task_actual_complete_date_key,task_date_key,measurement_type_key, defects_found from task_status_fact_hist left join task_date_fact_hist on task_status_fact_hist.plan_item_key = task_date_fact_hist.plan_item_key left join plan_item_hist on task_status_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on plan_item_hist.phase_key = phase.phase_key left join (select sum(defect_fix_count) as defects_found, plan_item_key from defect_log_fact_hist group by plan_item_key) as defect_table on defect_table.plan_item_key = task_status_fact_hist.plan_item_key")
tab_ev_info<-dbGetQuery(con, "select project_key,phase_base.phase_base_key,phase_base.phase_short_name,phase_type,wbs_element_key,task_actual_time_minutes,task_plan_time_minutes,task_actual_complete_date_key,task_date_key,measurement_type_key, defects_found from task_status_fact_hist left join task_date_fact_hist on task_status_fact_hist.plan_item_key = task_date_fact_hist.plan_item_key left join plan_item_hist on task_status_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on plan_item_hist.phase_key = phase.phase_key left join phase_mapping on phase.phase_key = phase_mapping.phase_key left join phase_base on phase_mapping.phase_base_key = phase_base.phase_base_key left join (select sum(defect_fix_count) as defects_found, plan_item_key from defect_log_fact_hist group by plan_item_key) as defect_table on defect_table.plan_item_key = task_status_fact_hist.plan_item_key")
tab_bcws_info<-dbGetQuery(con, "SELECT project_key, SUM(s.task_plan_time_minutes) as sum_plan_minutes FROM task_status_fact s, task_date_fact d, measurement_type t, plan_item_hist h WHERE s.plan_item_key = d.plan_item_key AND s.data_block_key = d.data_block_key AND d.measurement_type_key = t.measurement_type_key AND t.measurement_type_name = 'Plan' AND d.task_date_key <= 29991231 AND s.plan_item_key = h.plan_item_key group by project_key")
tab_size_info<-dbGetQuery(con, "select project_key,measurement_type_key,size_metric_name,sum(size_added_and_modified) as sum_size_am,sum(size_added) as sum_size_added,sum(size_base) as sum_size_base,sum(size_deleted) as sum_size_deleted,sum(size_modified) as sum_size_modified,sum(size_reused) as sum_size_reused,sum(size_total) as sum_size_total from size_fact_hist join plan_item_hist on size_fact_hist.plan_item_key = plan_item_hist.plan_item_key join size_metric on size_fact_hist.size_metric_key = size_metric.size_metric_key group by project_key,measurement_type_key,size_fact_hist.size_metric_key");
#tab_defect_injected_info<-dbGetQuery(con,"select project_key,sum(defect_fix_count) as sum_defect_fix_count,count(defect_log_fact_key) as sum_defect_records ,phase_short_name as defect_injected_phase_name from defect_log_fact_hist left join plan_item_hist on defect_log_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on defect_log_fact_hist.defect_injected_phase_key = phase.phase_key group by project_key,defect_injected_phase_key")
tab_defect_injected_info<-dbGetQuery(con,"select project_key,sum(defect_fix_count) as sum_defect_fix_count,count(defect_log_fact_key) as sum_defect_records ,phase_base.phase_short_name as defect_injected_phase_name from defect_log_fact_hist left join plan_item_hist on defect_log_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on defect_log_fact_hist.defect_injected_phase_key = phase.phase_key left join phase_mapping on phase.phase_key = phase_mapping.phase_key left join phase_base on phase_mapping.phase_base_key = phase_base.phase_base_key group by project_key,phase_base.phase_base_key")
#tab_defect_removed_info<-dbGetQuery(con,"select project_key,sum(defect_fix_count) as sum_defect_fix_count,count(defect_log_fact_key) as sum_defect_records ,phase_short_name as defect_removed_phase_name from defect_log_fact_hist left join plan_item_hist on defect_log_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on defect_log_fact_hist.defect_removed_phase_key = phase.phase_key group by project_key,defect_removed_phase_key")
tab_defect_removed_info<-dbGetQuery(con,"select project_key,sum(defect_fix_count) as sum_defect_fix_count,count(defect_log_fact_key) as sum_defect_records ,phase_base.phase_short_name as defect_removed_phase_name from defect_log_fact_hist left join plan_item_hist on defect_log_fact_hist.plan_item_key = plan_item_hist.plan_item_key left join phase on defect_log_fact_hist.defect_removed_phase_key = phase.phase_key left join phase_mapping on phase.phase_key = phase_mapping.phase_key left join phase_base on phase_mapping.phase_base_key = phase_base.phase_base_key group by project_key,phase_base.phase_base_key")
#tab_defect_fix_time_info<-dbGetQuery(con,"select project_key,plan_item.phase_key,phase_short_name,sum(defect_fix_time_minutes) as sum_defect_fix_time from defect_log_fact_hist left join plan_item on defect_log_fact_hist.plan_item_key = plan_item.plan_item_key left join phase on plan_item.phase_key = phase.phase_key group by project_key,phase_key")
tab_defect_fix_time_info<-dbGetQuery(con,"select project_key,phase_base.phase_base_key,phase_base.phase_short_name,sum(defect_fix_time_minutes) as sum_defect_fix_time from defect_log_fact_hist left join plan_item on defect_log_fact_hist.plan_item_key = plan_item.plan_item_key left join phase on plan_item.phase_key = phase.phase_key left join phase_mapping on phase.phase_key = phase_mapping.phase_key left join phase_base on phase_mapping.phase_base_key = phase_base.phase_base_key group by project_key,phase_base.phase_base_key")

# Read data selection from text file
fact_selection <- read.table("select_project-fact_data.txt", header=T, comment.char="#", sep=",")
fidelity_selection <- read.table("select_project-fidelity_data.txt", header=T, comment.char="#", sep=",")
selection_flgs <- list(fact_selection, fidelity_selection)
names(selection_flgs) <- c("fact_selection", "fidelity_selection")

# Check the existence of the text file for project selection
if (file.access("select_projects.txt") != 0) {
  unit <- unique(tab_project_info$project_key)
} else {
  # Read project selection from text file
  pj_selection <- read.table("select_projects.txt", header=T, comment.char="#")
  if (length(pj_selection$project_key) == 0) {
    unit <- unique(tab_project_info$project_key)
  } else {
    unit <- unique(pj_selection$project_key)
    
    tab_project_info <- subset(tab_project_info, project_key %in% pj_selection$project_key)
    tab_organization_info <- subset(tab_organization_info, project_key %in% pj_selection$project_key)
    tab_process_info <- subset(tab_process_info, project_key %in% pj_selection$project_key)
    tab_teams_info <- subset(tab_teams_info, project_key %in% pj_selection$project_key)
    tab_duration_info <- subset(tab_duration_info, project_key %in% pj_selection$project_key)
    tab_time_info <- subset(tab_time_info, project_key %in% pj_selection$project_key)
    tab_time_log_info <- subset(tab_time_log_info, project_key %in% pj_selection$project_key)
    tab_task_completion_info <- subset(tab_task_completion_info, project_key %in% pj_selection$project_key)
    tab_ev_info <- subset(tab_ev_info, project_key %in% pj_selection$project_key)
    tab_bcws_info <- subset(tab_bcws_info, project_key %in% pj_selection$project_key)
    tab_size_info <- subset(tab_size_info, project_key %in% pj_selection$project_key)
    tab_defect_injected_info <- subset(tab_defect_injected_info, project_key %in% pj_selection$project_key)
    tab_defect_removed_info <- subset(tab_defect_removed_info, project_key %in% pj_selection$project_key)
    tab_defect_fix_time_info <- subset(tab_defect_fix_time_info, project_key %in% pj_selection$project_key)
  }  
}

# Get data frame for project fact and project process fidelity
DF_list <- list(tab_project_info, tab_organization_info, tab_process_info, tab_teams_info, tab_duration_info, tab_time_info, tab_time_log_info, tab_ev_info, tab_bcws_info, tab_size_info, tab_defect_injected_info, tab_defect_removed_info, tab_defect_fix_time_info, tab_task_completion_info)
names(DF_list) <- c("tab_project_info", "tab_organization_info","tab_process_info", "tab_teams_info", "tab_duration_info", "tab_time_info", "tab_time_log_info", "tab_ev_info", "tab_bcws_info", "tab_size_info", "tab_defect_injected_info", "tab_defect_removed_info", "tab_defect_fix_time_info", "tab_task_completion_info") 

source("getFactDataFrame.R")
getFactDataFrame(unit, DF_list, selection_flgs, currentDirectory, "project")
}