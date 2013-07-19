/*****************************************************************************\
 *  common_as.c - common functions for accounting storage
 *
 *  $Id: common_as.c 13061 2008-01-22 21:23:56Z da $
 *****************************************************************************
 *  Copyright (C) 2004-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include "src/common/slurmdbd_defs.h"
#include "src/common/slurm_auth.h"
#include "src/common/xstring.h"
#include "src/common/env.h"
#include "src/slurmdbd/read_config.h"
#include "common_as.h"

extern char *assoc_hour_table;
extern char *assoc_day_table;
extern char *assoc_month_table;

extern char *cluster_hour_table;
extern char *cluster_day_table;
extern char *cluster_month_table;

extern char *wckey_hour_table;
extern char *wckey_day_table;
extern char *wckey_month_table;

/*
 * We want SLURMDB_MODIFY_ASSOC always to be the last
 */
static int _sort_update_object_dec(void *a, void *b)
{
	slurmdb_update_object_t *object_a = *(slurmdb_update_object_t **)a;
	slurmdb_update_object_t *object_b = *(slurmdb_update_object_t **)b;

	if ((object_a->type == SLURMDB_MODIFY_ASSOC)
	    && (object_b->type != SLURMDB_MODIFY_ASSOC))
		return 1;
	else if ((object_b->type == SLURMDB_MODIFY_ASSOC)
		&& (object_a->type != SLURMDB_MODIFY_ASSOC))
		return -1;
	return 0;
}

static void _dump_slurmdb_assoc_records(List assoc_list)
{
	slurmdb_association_rec_t *assoc = NULL;
	ListIterator itr = NULL;

	itr = list_iterator_create(assoc_list);
	while((assoc = list_next(itr))) {
		debug("\t\tid=%d", assoc->id);
	}
	list_iterator_destroy(itr);
}

/*
 * addto_update_list - add object updated to list
 * IN/OUT update_list: list of updated objects
 * IN type: update type
 * IN object: object updated
 * RET: error code
 *
 * NOTE: This function will take the object given and free it later so it
 *       needed to be removed from a list if in one before.
 */
extern int addto_update_list(List update_list, slurmdb_update_type_t type,
			     void *object)
{
	slurmdb_update_object_t *update_object = NULL;
	slurmdb_association_rec_t *assoc = object;
	slurmdb_qos_rec_t *qos = object;
	ListIterator itr = NULL;
	if (!update_list) {
		error("no update list given");
		return SLURM_ERROR;
	}

	itr = list_iterator_create(update_list);
	while((update_object = list_next(itr))) {
		if (update_object->type == type)
			break;
	}
	list_iterator_destroy(itr);

	if (update_object) {
		/* here we prepend primarly for remove association
		   since parents need to be removed last, and they are
		   removed first in the calling code */
		list_prepend(update_object->objects, object);
		return SLURM_SUCCESS;
	}
	update_object = xmalloc(sizeof(slurmdb_update_object_t));

	list_append(update_list, update_object);

	update_object->type = type;

	list_sort(update_list, (ListCmpF)_sort_update_object_dec);

	switch(type) {
	case SLURMDB_MODIFY_USER:
	case SLURMDB_ADD_USER:
	case SLURMDB_REMOVE_USER:
	case SLURMDB_ADD_COORD:
	case SLURMDB_REMOVE_COORD:
		update_object->objects = list_create(slurmdb_destroy_user_rec);
		break;
	case SLURMDB_ADD_ASSOC:
		/* We are going to send these to the slurmctld's so
		   lets set up the correct limits to INIFINITE instead
		   of NO_VAL */
		if (assoc->grp_cpu_mins == (uint64_t)NO_VAL)
			assoc->grp_cpu_mins = (uint64_t)INFINITE;
		if (assoc->grp_cpu_run_mins == (uint64_t)NO_VAL)
			assoc->grp_cpu_run_mins = (uint64_t)INFINITE;
		if (assoc->grp_cpus == NO_VAL)
			assoc->grp_cpus = INFINITE;
		if (assoc->grp_jobs == NO_VAL)
			assoc->grp_jobs = INFINITE;
		if (assoc->grp_mem == NO_VAL)
			assoc->grp_mem = INFINITE;
		if (assoc->grp_nodes == NO_VAL)
			assoc->grp_nodes = INFINITE;
		if (assoc->grp_submit_jobs == NO_VAL)
			assoc->grp_submit_jobs = INFINITE;
		if (assoc->grp_wall == NO_VAL)
			assoc->grp_wall = INFINITE;

		if (assoc->max_cpu_mins_pj == (uint64_t)NO_VAL)
			assoc->max_cpu_mins_pj = (uint64_t)INFINITE;
		if (assoc->max_cpu_run_mins == (uint64_t)NO_VAL)
			assoc->max_cpu_run_mins = (uint64_t)INFINITE;
		if (assoc->max_cpus_pj == NO_VAL)
			assoc->max_cpus_pj = INFINITE;
		if (assoc->max_jobs == NO_VAL)
			assoc->max_jobs = INFINITE;
		if (assoc->max_nodes_pj == NO_VAL)
			assoc->max_nodes_pj = INFINITE;
		if (assoc->max_submit_jobs == NO_VAL)
			assoc->max_submit_jobs = INFINITE;
		if (assoc->max_wall_pj == NO_VAL)
			assoc->max_wall_pj = INFINITE;
	case SLURMDB_MODIFY_ASSOC:
	case SLURMDB_REMOVE_ASSOC:
		xassert(((slurmdb_association_rec_t *)object)->cluster);
		update_object->objects = list_create(
			slurmdb_destroy_association_rec);
		break;
	case SLURMDB_ADD_QOS:
		/* We are going to send these to the slurmctld's so
		   lets set up the correct limits to INIFINITE instead
		   of NO_VAL */
		if (qos->grp_cpu_mins == (uint64_t)NO_VAL)
			qos->grp_cpu_mins = (uint64_t)INFINITE;
		if (qos->grp_cpu_run_mins == (uint64_t)NO_VAL)
			qos->grp_cpu_run_mins = (uint64_t)INFINITE;
		if (qos->grp_cpus == NO_VAL)
			qos->grp_cpus = INFINITE;
		if (qos->grp_jobs == NO_VAL)
			qos->grp_jobs = INFINITE;
		if (qos->grp_mem == NO_VAL)
			qos->grp_mem = INFINITE;
		if (qos->grp_nodes == NO_VAL)
			qos->grp_nodes = INFINITE;
		if (qos->grp_submit_jobs == NO_VAL)
			qos->grp_submit_jobs = INFINITE;
		if (qos->grp_wall == NO_VAL)
			qos->grp_wall = INFINITE;

		if (qos->max_cpu_mins_pj == (uint64_t)NO_VAL)
			qos->max_cpu_mins_pj = (uint64_t)INFINITE;
		if (qos->max_cpu_run_mins_pu == (uint64_t)NO_VAL)
			qos->max_cpu_run_mins_pu = (uint64_t)INFINITE;
		if (qos->max_cpus_pj == NO_VAL)
			qos->max_cpus_pj = INFINITE;
		if (qos->max_cpus_pu == NO_VAL)
			qos->max_cpus_pu = INFINITE;
		if (qos->max_jobs_pu == NO_VAL)
			qos->max_jobs_pu = INFINITE;
		if (qos->max_nodes_pj == NO_VAL)
			qos->max_nodes_pj = INFINITE;
		if (qos->max_nodes_pu == NO_VAL)
			qos->max_nodes_pu = INFINITE;
		if (qos->max_submit_jobs_pu == NO_VAL)
			qos->max_submit_jobs_pu = INFINITE;
		if (qos->max_wall_pj == NO_VAL)
			qos->max_wall_pj = INFINITE;
	case SLURMDB_MODIFY_QOS:
	case SLURMDB_REMOVE_QOS:
		update_object->objects = list_create(
			slurmdb_destroy_qos_rec);
		break;
	case SLURMDB_ADD_WCKEY:
	case SLURMDB_MODIFY_WCKEY:
	case SLURMDB_REMOVE_WCKEY:
		xassert(((slurmdb_wckey_rec_t *)object)->cluster);
		update_object->objects = list_create(
			slurmdb_destroy_wckey_rec);
		break;
	case SLURMDB_ADD_CLUSTER:
	case SLURMDB_REMOVE_CLUSTER:
		/* This should only be the name of the cluster, and is
		   only used in the plugin for rollback purposes.
		*/
		update_object->objects = list_create(slurm_destroy_char);
		break;
	case SLURMDB_UPDATE_NOTSET:
	default:
		error("unknown type set in update_object: %d", type);
		return SLURM_ERROR;
	}
	debug4("XXX: update object with type %d added", type);
	list_append(update_object->objects, object);
	return SLURM_SUCCESS;
}

/*
 * dump_update_list - dump contents of updates
 * IN update_list: updates to perform
 */
extern void dump_update_list(List update_list)
{
	ListIterator itr = NULL;
	slurmdb_update_object_t *object = NULL;

	debug3("========== DUMP UPDATE LIST ==========");
	itr = list_iterator_create(update_list);
	while((object = list_next(itr))) {
		if (!object->objects || !list_count(object->objects)) {
			debug3("\tUPDATE OBJECT WITH NO RECORDS, type: %d",
			       object->type);
			continue;
		}
		switch(object->type) {
		case SLURMDB_MODIFY_USER:
		case SLURMDB_ADD_USER:
		case SLURMDB_REMOVE_USER:
		case SLURMDB_ADD_COORD:
		case SLURMDB_REMOVE_COORD:
			debug3("\tUSER RECORDS");
			break;
		case SLURMDB_ADD_ASSOC:
		case SLURMDB_MODIFY_ASSOC:
		case SLURMDB_REMOVE_ASSOC:
			debug3("\tASSOC RECORDS");
			_dump_slurmdb_assoc_records(object->objects);
			break;
		case SLURMDB_ADD_QOS:
		case SLURMDB_MODIFY_QOS:
		case SLURMDB_REMOVE_QOS:
			debug3("\tQOS RECORDS");
			break;
		case SLURMDB_ADD_WCKEY:
		case SLURMDB_MODIFY_WCKEY:
		case SLURMDB_REMOVE_WCKEY:
			debug3("\tWCKEY RECORDS");
			break;
		case SLURMDB_UPDATE_NOTSET:
		default:
			error("unknown type set in "
			      "update_object: %d",
			      object->type);
			break;
		}
	}
	list_iterator_destroy(itr);
}


/*
 * cluster_first_reg - ask for controller to send nodes in a down state
 *    and jobs pending or running on first registration.
 *
 * IN host: controller host
 * IN port: controller port
 * IN rpc_version: controller rpc version
 * RET: error code
 */
extern int cluster_first_reg(char *host, uint16_t port, uint16_t rpc_version)
{
	slurm_addr_t ctld_address;
	slurm_fd_t fd;
	int rc = SLURM_SUCCESS;

	info("First time to register cluster requesting "
	     "running jobs and system information.");

	slurm_set_addr_char(&ctld_address, port, host);
	fd = slurm_open_msg_conn(&ctld_address);
	if (fd < 0) {
		error("can not open socket back to slurmctld "
		      "%s(%u): %m", host, port);
		rc = SLURM_ERROR;
	} else {
		slurm_msg_t out_msg;
		accounting_update_msg_t update;
		/* We have to put this update message here so
		   we can tell the sender to send the correct
		   RPC version.
		*/
		memset(&update, 0, sizeof(accounting_update_msg_t));
		update.rpc_version = rpc_version;
		slurm_msg_t_init(&out_msg);
		out_msg.msg_type = ACCOUNTING_FIRST_REG;
		out_msg.flags = SLURM_GLOBAL_AUTH_KEY;
		out_msg.data = &update;
		slurm_send_node_msg(fd, &out_msg);
		/* We probably need to add matching recv_msg function
		 * for an arbitray fd or should these be fire
		 * and forget?  For this, that we can probably
		 * forget about it */
		slurm_close_stream(fd);
	}
	return rc;
}

/*
 * set_usage_information - set time and table information for getting usage
 *
 * OUT usage_table: which usage table to query
 * IN type: usage type to get
 * IN/OUT usage_start: start time
 * IN/OUT usage_end: end time
 * RET: error code
 */
extern int set_usage_information(char **usage_table, slurmdbd_msg_type_t type,
				 time_t *usage_start, time_t *usage_end)
{
	time_t start = (*usage_start), end = (*usage_end);
	time_t my_time = time(NULL);
	struct tm start_tm;
	struct tm end_tm;
	char *my_usage_table = (*usage_table);

	/* Default is going to be the last day */
	if (!end) {
		if (!localtime_r(&my_time, &end_tm)) {
			error("Couldn't get localtime from end %ld",
			      my_time);
			return SLURM_ERROR;
		}
		end_tm.tm_hour = 0;
	} else {
		if (!localtime_r(&end, &end_tm)) {
			error("Couldn't get localtime from user end %ld",
			      end);
			return SLURM_ERROR;
		}
	}
	end_tm.tm_sec = 0;
	end_tm.tm_min = 0;
	end_tm.tm_isdst = -1;
	end = mktime(&end_tm);

	if (!start) {
		if (!localtime_r(&my_time, &start_tm)) {
			error("Couldn't get localtime from start %ld",
			      my_time);
			return SLURM_ERROR;
		}
		start_tm.tm_hour = 0;
		start_tm.tm_mday--;
	} else {
		if (!localtime_r(&start, &start_tm)) {
			error("Couldn't get localtime from user start %ld",
			      start);
			return SLURM_ERROR;
		}
	}
	start_tm.tm_sec = 0;
	start_tm.tm_min = 0;
	start_tm.tm_isdst = -1;
	start = mktime(&start_tm);

	if (end-start < 3600) {
		end = start + 3600;
		if (!localtime_r(&end, &end_tm)) {
			error("2 Couldn't get localtime from user end %ld",
			      end);
			return SLURM_ERROR;
		}
	}
	/* check to see if we are off day boundaries or on month
	 * boundaries other wise use the day table.
	 */
	//info("%d %d %d", start_tm.tm_hour, end_tm.tm_hour, end-start);
	if (start_tm.tm_hour || end_tm.tm_hour || (end-start < 86400)
	   || (end > my_time)) {
		switch (type) {
		case DBD_GET_ASSOC_USAGE:
			my_usage_table = assoc_hour_table;
			break;
		case DBD_GET_WCKEY_USAGE:
			my_usage_table = wckey_hour_table;
			break;
		case DBD_GET_CLUSTER_USAGE:
			my_usage_table = cluster_hour_table;
			break;
		default:
			error("Bad type given for hour usage %d %s", type,
			     slurmdbd_msg_type_2_str(type, 1));
			break;
		}
	} else if (start_tm.tm_mday == 0 && end_tm.tm_mday == 0
		  && (end-start > 86400)) {
		switch (type) {
		case DBD_GET_ASSOC_USAGE:
			my_usage_table = assoc_month_table;
			break;
		case DBD_GET_WCKEY_USAGE:
			my_usage_table = wckey_month_table;
			break;
		case DBD_GET_CLUSTER_USAGE:
			my_usage_table = cluster_month_table;
			break;
		default:
			error("Bad type given for month usage %d %s", type,
			     slurmdbd_msg_type_2_str(type, 1));
			break;
		}
	}

	(*usage_start) = start;
	(*usage_end) = end;
	(*usage_table) = my_usage_table;
	return SLURM_SUCCESS;
}


/*
 * merge_delta_qos_list - apply delta_qos_list to qos_list
 *
 * IN/OUT qos_list: list of QOS'es
 * IN delta_qos_list: list of delta QOS'es
 */
extern void merge_delta_qos_list(List qos_list, List delta_qos_list)
{
	ListIterator curr_itr = list_iterator_create(qos_list);
	ListIterator new_itr = list_iterator_create(delta_qos_list);
	char *new_qos = NULL, *curr_qos = NULL;

	while((new_qos = list_next(new_itr))) {
		if (new_qos[0] == '-') {
			while((curr_qos = list_next(curr_itr))) {
				if (!strcmp(curr_qos, new_qos+1)) {
					list_delete_item(curr_itr);
					break;
				}
			}
			list_iterator_reset(curr_itr);
		} else if (new_qos[0] == '+') {
			while((curr_qos = list_next(curr_itr))) {
				if (!strcmp(curr_qos, new_qos+1)) {
					break;
				}
			}
			if (!curr_qos) {
				list_append(qos_list, xstrdup(new_qos+1));
			}
			list_iterator_reset(curr_itr);
		}
	}
	list_iterator_destroy(new_itr);
	list_iterator_destroy(curr_itr);
}

extern bool is_user_min_admin_level(void *db_conn, uid_t uid,
				    slurmdb_admin_level_t min_level)
{
	bool is_admin = 1;
	/* This only works when running though the slurmdbd.
	 * THERE IS NO AUTHENTICATION WHEN RUNNNING OUT OF THE
	 * SLURMDBD!
	 */
	if (slurmdbd_conf) {
		/* We have to check the authentication here in the
		 * plugin since we don't know what accounts are being
		 * referenced until after the query.
		 */
		if ((uid != slurmdbd_conf->slurm_user_id && uid != 0)
		   && assoc_mgr_get_admin_level(db_conn, uid) < min_level)
			is_admin = 0;
	} else if (uid != 0)
		is_admin = 0;

	return is_admin;
}

extern bool is_user_coord(slurmdb_user_rec_t *user, char *account)
{
	ListIterator itr;
	slurmdb_coord_rec_t *coord;

	xassert(user);
	xassert(account);

	if (!user->coord_accts || !list_count(user->coord_accts))
		return 0;

	itr = list_iterator_create(user->coord_accts);
	while((coord = list_next(itr))) {
		if (!strcasecmp(coord->name, account))
			break;
	}
	list_iterator_destroy(itr);
	return coord ? 1 : 0;
}

extern bool is_user_any_coord(void *db_conn, slurmdb_user_rec_t *user)
{
	xassert(user);
	if (assoc_mgr_fill_in_user(db_conn, user, 1, NULL) != SLURM_SUCCESS) {
		error("couldn't get information for this user %s(%d)",
		      user->name, user->uid);
		return 0;
	}
	return (user->coord_accts && list_count(user->coord_accts));
}

/*
 * acct_get_db_name - get database name of accouting storage
 * RET: database name, should be free-ed by caller
 */
extern char *acct_get_db_name(void)
{
	char *db_name = NULL;
	char *location = slurm_get_accounting_storage_loc();

	if (!location)
		db_name = xstrdup(DEFAULT_ACCOUNTING_DB);
	else {
		int i = 0;
		while(location[i]) {
			if (location[i] == '.' || location[i] == '/') {
				debug("%s doesn't look like a database "
				      "name using %s",
				      location, DEFAULT_ACCOUNTING_DB);
				break;
			}
			i++;
		}
		if (location[i]) {
			db_name = xstrdup(DEFAULT_ACCOUNTING_DB);
			xfree(location);
		} else
			db_name = location;
	}
	return db_name;
}

extern time_t archive_setup_end_time(time_t last_submit, uint32_t purge)
{
	struct tm time_tm;
	int16_t units;

	if (purge == NO_VAL) {
		error("Invalid purge set");
		return 0;
	}

	units = SLURMDB_PURGE_GET_UNITS(purge);
	if (units < 0) {
		error("invalid units from purge '%d'", units);
		return 0;
	}

	/* use localtime to avoid any daylight savings issues */
	if (!localtime_r(&last_submit, &time_tm)) {
		error("Couldn't get localtime from first "
		      "suspend start %ld", (long)last_submit);
		return 0;
	}

	time_tm.tm_sec = 0;
	time_tm.tm_min = 0;

	if (SLURMDB_PURGE_IN_HOURS(purge))
		time_tm.tm_hour -= units;
	else if (SLURMDB_PURGE_IN_DAYS(purge)) {
		time_tm.tm_hour = 0;
		time_tm.tm_mday -= units;
	} else if (SLURMDB_PURGE_IN_MONTHS(purge)) {
		time_tm.tm_hour = 0;
		time_tm.tm_mday = 1;
		time_tm.tm_mon -= units;
	} else {
		errno = EINVAL;
		error("No known unit given for purge, "
		      "we are guessing mistake and returning error");
		return 0;
	}

	time_tm.tm_isdst = -1;
	return (mktime(&time_tm) - 1);
}


/* execute archive script */
extern int archive_run_script(slurmdb_archive_cond_t *arch_cond,
		   char *cluster_name, time_t last_submit)
{
	char * args[] = {arch_cond->archive_script, NULL};
	struct stat st;
	char **env = NULL;
	time_t curr_end;

	if (stat(arch_cond->archive_script, &st) < 0) {
		errno = errno;
		error("archive_run_script: failed to stat %s: %m",
		      arch_cond->archive_script);
		return SLURM_ERROR;
	}

	if (!(st.st_mode & S_IFREG)) {
		errno = EACCES;
		error("archive_run_script: %s isn't a regular file",
		      arch_cond->archive_script);
		return SLURM_ERROR;
	}

	if (access(arch_cond->archive_script, X_OK) < 0) {
		errno = EACCES;
		error("archive_run_script: %s is not executable",
		      arch_cond->archive_script);
		return SLURM_ERROR;
	}

	env = env_array_create();
	env_array_append_fmt(&env, "SLURM_ARCHIVE_CLUSTER", "%s",
			     cluster_name);

	if (arch_cond->purge_event != NO_VAL) {
		if (!(curr_end = archive_setup_end_time(
			     last_submit, arch_cond->purge_event))) {
			error("Parsing purge events failed");
			return SLURM_ERROR;
		}

		env_array_append_fmt(&env, "SLURM_ARCHIVE_EVENTS", "%u",
				     SLURMDB_PURGE_ARCHIVE_SET(
					     arch_cond->purge_event));
		env_array_append_fmt(&env, "SLURM_ARCHIVE_LAST_EVENT", "%ld",
				     (long)curr_end);
	}

	if (arch_cond->purge_job != NO_VAL) {
		if (!(curr_end = archive_setup_end_time(
			     last_submit, arch_cond->purge_job))) {
			error("Parsing purge job failed");
			return SLURM_ERROR;
		}

		env_array_append_fmt(&env, "SLURM_ARCHIVE_JOBS", "%u",
				     SLURMDB_PURGE_ARCHIVE_SET(
					     arch_cond->purge_job));
		env_array_append_fmt(&env, "SLURM_ARCHIVE_LAST_JOB", "%ld",
				     (long)curr_end);
	}

	if (arch_cond->purge_resv != NO_VAL) {
		if (!(curr_end = archive_setup_end_time(
			     last_submit, arch_cond->purge_job))) {
			error("Parsing purge job failed");
			return SLURM_ERROR;
		}

		env_array_append_fmt(&env, "SLURM_ARCHIVE_RESV", "%u",
				     SLURMDB_PURGE_ARCHIVE_SET(
					     arch_cond->purge_job));
		env_array_append_fmt(&env, "SLURM_ARCHIVE_LAST_RESV", "%ld",
				     (long)curr_end);
	}

	if (arch_cond->purge_step != NO_VAL) {
		if (!(curr_end = archive_setup_end_time(
			     last_submit, arch_cond->purge_step))) {
			error("Parsing purge step");
			return SLURM_ERROR;
		}

		env_array_append_fmt(&env, "SLURM_ARCHIVE_STEPS", "%u",
				     SLURMDB_PURGE_ARCHIVE_SET(
					     arch_cond->purge_step));
		env_array_append_fmt(&env, "SLURM_ARCHIVE_LAST_STEP", "%ld",
				     (long)curr_end);
	}

	if (arch_cond->purge_suspend != NO_VAL) {
		if (!(curr_end = archive_setup_end_time(
			     last_submit, arch_cond->purge_suspend))) {
			error("Parsing purge suspend");
			return SLURM_ERROR;
		}

		env_array_append_fmt(&env, "SLURM_ARCHIVE_SUSPEND", "%u",
				     SLURMDB_PURGE_ARCHIVE_SET(
					     arch_cond->purge_suspend));
		env_array_append_fmt(&env, "SLURM_ARCHIVE_LAST_SUSPEND", "%ld",
				     (long)curr_end);
	}

#ifdef _PATH_STDPATH
	env_array_append (&env, "PATH", _PATH_STDPATH);
#else
	env_array_append (&env, "PATH", "/bin:/usr/bin");
#endif
	execve(arch_cond->archive_script, args, env);

	env_array_free(env);

	return SLURM_SUCCESS;
}

static char *_make_archive_name(time_t period_start, time_t period_end,
				char *cluster_name, char *arch_dir,
				char *arch_type, uint32_t archive_period)
{
	struct tm time_tm;
	char start_char[32];
	char end_char[32];

	localtime_r((time_t *)&period_start, &time_tm);
	time_tm.tm_sec = 0;
	time_tm.tm_min = 0;

	/* set up the start time based off the period we are purging */
	if (SLURMDB_PURGE_IN_HOURS(archive_period)) {
	} else if (SLURMDB_PURGE_IN_DAYS(archive_period)) {
		time_tm.tm_hour = 0;
	} else {
		time_tm.tm_hour = 0;
		time_tm.tm_mday = 1;
	}

	snprintf(start_char, sizeof(start_char),
		 "%4.4u-%2.2u-%2.2u"
		 "T%2.2u:%2.2u:%2.2u",
		 (time_tm.tm_year + 1900),
		 (time_tm.tm_mon+1),
		 time_tm.tm_mday,
		 time_tm.tm_hour,
		 time_tm.tm_min,
		 time_tm.tm_sec);

	localtime_r((time_t *)&period_end, &time_tm);
	snprintf(end_char, sizeof(end_char),
		 "%4.4u-%2.2u-%2.2u"
		 "T%2.2u:%2.2u:%2.2u",
		 (time_tm.tm_year + 1900),
		 (time_tm.tm_mon+1),
		 time_tm.tm_mday,
		 time_tm.tm_hour,
		 time_tm.tm_min,
		 time_tm.tm_sec);

	/* write the buffer to file */
	return xstrdup_printf("%s/%s_%s_archive_%s_%s",
			      arch_dir, cluster_name, arch_type,
			      start_char, end_char);
}

extern int archive_write_file(Buf buffer, char *cluster_name,
			      time_t period_start, time_t period_end,
			      char *arch_dir, char *arch_type,
			      uint32_t archive_period)
{
	int fd = 0;
	int rc = SLURM_SUCCESS;
	char *old_file = NULL, *new_file = NULL, *reg_file = NULL;
	static int high_buffer_size = (1024 * 1024);
	static pthread_mutex_t local_file_lock = PTHREAD_MUTEX_INITIALIZER;

	xassert(buffer);

	slurm_mutex_lock(&local_file_lock);

	/* write the buffer to file */
	reg_file = _make_archive_name(period_start, period_end,
				      cluster_name, arch_dir,
				      arch_type, archive_period);

	debug("Storing %s archive for %s at %s",
	      arch_type, cluster_name, reg_file);
	old_file = xstrdup_printf("%s.old", reg_file);
	new_file = xstrdup_printf("%s.new", reg_file);

	fd = creat(new_file, 0600);
	if (fd < 0) {
		error("Can't save archive, create file %s error %m", new_file);
		rc = SLURM_ERROR;
	} else {
		int pos = 0, nwrite = get_buf_offset(buffer), amount;
		char *data = (char *)get_buf_data(buffer);
		high_buffer_size = MAX(nwrite, high_buffer_size);
		while (nwrite > 0) {
			amount = write(fd, &data[pos], nwrite);
			if ((amount < 0) && (errno != EINTR)) {
				error("Error writing file %s, %m", new_file);
				rc = SLURM_ERROR;
				break;
			}
			nwrite -= amount;
			pos    += amount;
		}
		fsync(fd);
		close(fd);
	}

	if (rc)
		(void) unlink(new_file);
	else {			/* file shuffle */
		(void) unlink(old_file);
		if (link(reg_file, old_file))
			debug4("Link(%s, %s): %m", reg_file, old_file);
		(void) unlink(reg_file);
		if (link(new_file, reg_file))
			debug4("Link(%s, %s): %m", new_file, reg_file);
		(void) unlink(new_file);
	}
	xfree(old_file);
	xfree(reg_file);
	xfree(new_file);
	slurm_mutex_unlock(&local_file_lock);

	return rc;
}
