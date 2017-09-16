/*****************************************************************************\
 *  cli_filter_lua.c - lua CLI option processing specifications.
 *****************************************************************************
 *  Copyright (C) 2017 Regents of the University of California
 *  Produced at Lawrence Berkeley National Laboratory
 *  Written by Douglas Jacobsen <dmjacobsen@lbl.gov>
 *  All rights reserved.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
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

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dlfcn.h>
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"
#include "src/common/slurm_xlator.h"
#include "src/common/cli_filter.h"
#include "src/salloc/salloc_opt.h"
#include "src/sbatch/sbatch_opt.h"
#include "src/srun/libsrun/srun_opt.h"
#include "src/common/xlua.h"

/*
 * These variables are required by the generic plugin interface.  If they
 * are not found in the plugin, the plugin loader will ignore it.
 *
 * plugin_name - a string giving a human-readable description of the
 * plugin.  There is no maximum length, but the symbol must refer to
 * a valid string.
 *
 * plugin_type - a string suggesting the type of the plugin or its
 * applicability to a particular form of data or method of data handling.
 * If the low-level plugin API is used, the contents of this string are
 * unimportant and may be anything.  SLURM uses the higher-level plugin
 * interface which requires this string to be of the form
 *
 *	<application>/<method>
 *
 * where <application> is a description of the intended application of
 * the plugin (e.g., "auth" for SLURM authentication) and <method> is a
 * description of how this plugin satisfies that application.  SLURM will
 * only load authentication plugins if the plugin_type string has a prefix
 * of "auth/".
 *
 * plugin_version - an unsigned 32-bit integer containing the Slurm version
 * (major.minor.micro combined into a single number).
 */
const char plugin_name[]       	= "cli filter defaults plugin";
const char plugin_type[]       	= "cli_filter/lua";
const uint32_t plugin_version   = SLURM_VERSION_NUMBER;
static const char lua_script_path[] = DEFAULT_SCRIPT_DIR "/cli_filter.lua";
static lua_State *L = NULL;
static char *user_msg = NULL;

static int _log_lua_msg(lua_State *L);
static int _log_lua_error(lua_State *L);
static int _log_lua_user_msg(lua_State *L);
static int _load_script(void);
static void _stack_dump (char *header, lua_State *L);

static const struct luaL_Reg slurm_functions [] = {
	{ "log",	_log_lua_msg   },
	{ "error",	_log_lua_error },
	{ "user_msg",	_log_lua_user_msg },
	{ NULL,		NULL        }
};

struct option_string;
static bool _push_string(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_bool(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_int(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_int64_t(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_int32_t(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_uid(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_gid(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_stringarray(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);

static bool _write_string(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_bool(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_int(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_int64_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_int32_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_uid(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_gid(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_uint(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_uint(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_long(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_long(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_uint64_t(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_uint64_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_uint32_t(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L);
static bool _write_uint32_t(void *data, int idx, const char *name,
			 const struct option_string *opt_str, lua_State *L);
static bool _push_uint16_t(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L);
static bool _write_uint16_t(void *data, int idx, const char *name,
			 const struct option_string *opt_str, lua_State *L);
static bool _push_uint8_t(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_uint8_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _push_time_t(void *data, const char *name,
			const struct option_string *opt_str, lua_State *L);
static bool _write_time_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L);

struct option_string {
	char *name;
	size_t offset;
	bool (*read)(void *, const char *, const struct option_string *,
			lua_State *);
	bool (*write)(void *, int, const char *, const struct option_string *,
			lua_State *);
};

static struct option_string salloc_opt_names[] = {
	{ "progname", offsetof(salloc_opt_t, progname), _push_string, NULL },
	{ "user",     offsetof(salloc_opt_t, user), _push_string, NULL },
	{ "uid",      offsetof(salloc_opt_t, uid), _push_uid, NULL },
	{ "gid",      offsetof(salloc_opt_t, gid), _push_gid, NULL },
	{ "euid",     offsetof(salloc_opt_t, euid), _push_uid, NULL },
	{ "egid",     offsetof(salloc_opt_t, egid), _push_gid, NULL },
	{ "ntasks",   offsetof(salloc_opt_t, ntasks), _push_int, _write_int },
	{ "ntasks_set", offsetof(salloc_opt_t, ntasks_set), _push_bool, _write_bool },
	{ "cpus_per_task", offsetof(salloc_opt_t, cpus_per_task), _push_int, _write_int },
	{ "cpus_per_task_set", offsetof(salloc_opt_t, cpus_set), _push_bool, _write_bool },
	{ "min_nodes", offsetof(salloc_opt_t, min_nodes), _push_int, _write_int },
	{ "max_nodes", offsetof(salloc_opt_t, max_nodes), _push_int, _write_int },
	{ "nodes_set", offsetof(salloc_opt_t, nodes_set), _push_bool, _write_bool },
	{ "sockets_per_node", offsetof(salloc_opt_t, sockets_per_node), _push_int, _write_int },
	{ "cores_per_socket", offsetof(salloc_opt_t, cores_per_socket), _push_int, _write_int },
	{ "threads_per_core", offsetof(salloc_opt_t, threads_per_core), _push_int, _write_int },
	{ "ntasks_per_node", offsetof(salloc_opt_t, ntasks_per_node), _push_int, _write_int },
	{ "ntasks_per_socket", offsetof(salloc_opt_t, ntasks_per_socket), _push_int, _write_int },
	{ "ntasks_per_core", offsetof(salloc_opt_t, ntasks_per_core), _push_int, _write_int },
	{ "ntasks_per_core_set", offsetof(salloc_opt_t, ntasks_per_core_set), _push_bool, _write_bool },
	{ "hint", offsetof(salloc_opt_t, hint_env), _push_string, _write_string },
	{ "hint_set", offsetof(salloc_opt_t, hint_set), _push_bool, _write_bool },
	/* explicitly skipping mem_bind options for now since they are more complicated */
	{ "extra_set", offsetof(salloc_opt_t, extra_set), _push_bool, _write_bool },
	{ "time_limit", offsetof(salloc_opt_t, time_limit), _push_int, _write_int },
	{ "time_limit_str", offsetof(salloc_opt_t, time_limit_str), _push_string, _write_string },
	{ "time_min", offsetof(salloc_opt_t, time_min), _push_int, _write_int },
	{ "time_min_str", offsetof(salloc_opt_t, time_min_str), _push_string, _write_string },
	{ "partition", offsetof(salloc_opt_t, partition), _push_string, _write_string },
	/* explicitly skip distribution and plane, may require special handling */
	{ "job_name", offsetof(salloc_opt_t, job_name), _push_string, _write_string},
	{ "jobid", offsetof(salloc_opt_t, jobid), _push_uint, _write_uint },
	{ "dependency", offsetof(salloc_opt_t, dependency), _push_string, _write_string },
	{ "nice", offsetof(salloc_opt_t, nice), _push_int, _write_int },
	{ "priority", offsetof(salloc_opt_t, priority), _push_uint32_t, _write_uint32_t },
	{ "account", offsetof(salloc_opt_t, account), _push_string, _write_string },
	{ "comment", offsetof(salloc_opt_t, comment), _push_string, _write_string },
	{ "qos", offsetof(salloc_opt_t, qos), _push_string, _write_string },
	{ "immediate", offsetof(salloc_opt_t, immediate), _push_int, _write_int },
	{ "warn_flags", offsetof(salloc_opt_t, warn_flags), _push_uint16_t, _write_uint16_t },
	{ "warn_signal", offsetof(salloc_opt_t, warn_signal), _push_uint16_t, _write_uint16_t },
	{ "warn_time", offsetof(salloc_opt_t, warn_time), _push_uint16_t, _write_uint16_t },
	{ "hold", offsetof(salloc_opt_t, hold), _push_bool, _write_bool },
	{ "no_kill", offsetof(salloc_opt_t, no_kill), _push_bool, _write_bool },
	{ "acctg_freq", offsetof(salloc_opt_t, acctg_freq), _push_string, _write_string },
	{ "licenses", offsetof(salloc_opt_t, licenses), _push_string, _write_string },
	{ "overcommit", offsetof(salloc_opt_t, overcommit), _push_bool, _write_bool },
	{ "kill_command_signal", offsetof(salloc_opt_t, kill_command_signal), _push_int, _write_int },
	{ "kill_command_signal_set", offsetof(salloc_opt_t, kill_command_signal_set), _push_bool, _write_bool },
	{ "shared", offsetof(salloc_opt_t, shared), _push_uint16_t, _write_uint16_t},
	{ "quiet", offsetof(salloc_opt_t, quiet), _push_int, _write_int },
	{ "verbose", offsetof(salloc_opt_t, verbose), _push_int, _write_int },

	/* constraint options */
	{ "mincpus", offsetof(salloc_opt_t, mincpus), _push_int, _write_int },
	{ "mem_per_cpu", offsetof(salloc_opt_t, mem_per_cpu), _push_int64_t, _write_int64_t },
	{ "mem", offsetof(salloc_opt_t, realmem), _push_int64_t, _write_int64_t },
	{ "tmpdisk", offsetof(salloc_opt_t, tmpdisk), _push_long, _write_long },
	{ "constraints", offsetof(salloc_opt_t, constraints), _push_string, _write_string },
	{ "cluster_constraints", offsetof(salloc_opt_t, c_constraints), _push_string, _write_string },
	{ "gres", offsetof(salloc_opt_t, gres), _push_string, _write_string },
	{ "contiguous", offsetof(salloc_opt_t, contiguous), _push_bool, _write_bool },
	{ "nodelist", offsetof(salloc_opt_t, nodelist), _push_string, _write_string },
	{ "exc_nodes", offsetof(salloc_opt_t, exc_nodes), _push_string, _write_string },
	{ "network", offsetof(salloc_opt_t, network), _push_string, _write_string },

	/* only bluegene reboot option for now */
	{ "reboot", offsetof(salloc_opt_t, reboot), _push_bool, _write_bool },

	/* remaining options */
	{ "begin", offsetof(salloc_opt_t, begin), _push_time_t, _write_time_t },
	{ "mail_type", offsetof(salloc_opt_t, mail_type), _push_uint16_t, _write_uint16_t },
	{ "mail_user", offsetof(salloc_opt_t, mail_user), _push_string, _write_string },
	/* skip bell for now */
	{ "no_shell", offsetof(salloc_opt_t, no_shell), _push_bool, _write_bool },
	{ "get_user_env_time", offsetof(salloc_opt_t, get_user_env_time), _push_int, _write_int },
	{ "get_user_env_mode", offsetof(salloc_opt_t, get_user_env_mode), _push_int, _write_int },
	{ "cwd", offsetof(salloc_opt_t, cwd), _push_string, _write_string },
	{ "reservation", offsetof(salloc_opt_t, reservation), _push_string, _write_string },
	{ "wait_all_nodes", offsetof(salloc_opt_t, wait_all_nodes), _push_uint16_t, _write_uint16_t },
	{ "wckey", offsetof(salloc_opt_t, wckey), _push_string, _write_string },
	{ "req_switch", offsetof(salloc_opt_t, req_switch), _push_int, _write_int },
	{ "wait4switch", offsetof(salloc_opt_t, wait4switch), _push_int, _write_int },
	/* skip spank env for the moment -- TODO SOON! */
	{ "core_spec", offsetof(salloc_opt_t, core_spec), _push_int, _write_int },
	{ "burst_buffer", offsetof(salloc_opt_t, burst_buffer), _push_string, _write_string },
	{ "cpu_freq_min", offsetof(salloc_opt_t, cpu_freq_min), _push_uint32_t, _write_uint32_t },
	{ "cpu_freq_max", offsetof(salloc_opt_t, cpu_freq_max), _push_uint32_t, _write_uint32_t },
	{ "cpu_freq_gov", offsetof(salloc_opt_t, cpu_freq_gov), _push_uint32_t, _write_uint32_t },
	{ "power_flags", offsetof(salloc_opt_t, power_flags), _push_uint8_t, _write_uint8_t },
	{ "mcs_label", offsetof(salloc_opt_t, mcs_label), _push_string, _write_string },
	{ "deadline", offsetof(salloc_opt_t, deadline), _push_time_t, _write_time_t },
	{ "job_flags", offsetof(salloc_opt_t, job_flags), _push_uint32_t, _write_uint32_t },
	{ "delay_boot", offsetof(salloc_opt_t, delay_boot), _push_uint32_t, _write_uint32_t },
	{ NULL, 0, NULL, NULL }
};

static struct option_string sbatch_opt_names[] = {
	{ "clusters", offsetof(sbatch_opt_t, clusters), _push_string, _write_string },
	{ "progname", offsetof(sbatch_opt_t, progname), _push_string, NULL },
	{ "argc", offsetof(sbatch_opt_t, script_argc), _push_int, _write_int },
	{ "argv", offsetof(sbatch_opt_t, script_argv), _push_stringarray, NULL },
	{ "user",     offsetof(sbatch_opt_t, user), _push_string, NULL },
	{ "uid",      offsetof(sbatch_opt_t, uid), _push_uid, NULL },
	{ "gid",      offsetof(sbatch_opt_t, gid), _push_gid, NULL },
	{ "euid",     offsetof(sbatch_opt_t, euid), _push_uid, NULL },
	{ "egid",     offsetof(sbatch_opt_t, egid), _push_gid, NULL },
	{ "cwd",   offsetof(sbatch_opt_t, cwd), _push_string, _write_string },
	{ "ntasks",   offsetof(sbatch_opt_t, ntasks), _push_int, _write_int },
	{ "ntasks_set", offsetof(sbatch_opt_t, ntasks_set), _push_bool, _write_bool },
	{ "cpus_per_task", offsetof(sbatch_opt_t, cpus_per_task), _push_int, _write_int },
	{ "cpus_per_task_set", offsetof(sbatch_opt_t, cpus_set), _push_bool, _write_bool },
	{ "min_nodes", offsetof(sbatch_opt_t, min_nodes), _push_int, _write_int },
	{ "max_nodes", offsetof(sbatch_opt_t, max_nodes), _push_int, _write_int },
	{ "nodes_set", offsetof(sbatch_opt_t, nodes_set), _push_bool, _write_bool },
	{ "sockets_per_node", offsetof(sbatch_opt_t, sockets_per_node), _push_int, _write_int },
	{ "cores_per_socket", offsetof(sbatch_opt_t, cores_per_socket), _push_int, _write_int },
	{ "threads_per_core", offsetof(sbatch_opt_t, threads_per_core), _push_int, _write_int },
	{ "ntasks_per_node", offsetof(sbatch_opt_t, ntasks_per_node), _push_int, _write_int },
	{ "ntasks_per_socket", offsetof(sbatch_opt_t, ntasks_per_socket), _push_int, _write_int },
	{ "ntasks_per_core", offsetof(sbatch_opt_t, ntasks_per_core), _push_int, _write_int },
	{ "ntasks_per_core_set", offsetof(sbatch_opt_t, ntasks_per_core_set), _push_bool, _write_bool },
	{ "hint", offsetof(sbatch_opt_t, hint_env), _push_string, _write_string },
	{ "hint_set", offsetof(sbatch_opt_t, hint_set), _push_bool, _write_bool },
	/* explicitly skipping mem_bind options for now since they are more complicated */
	{ "extra_set", offsetof(sbatch_opt_t, extra_set), _push_bool, _write_bool },
	{ "time_limit", offsetof(sbatch_opt_t, time_limit), _push_int, _write_int },
	{ "time_limit_str", offsetof(sbatch_opt_t, time_limit_str), _push_string, _write_string },
	{ "time_min", offsetof(sbatch_opt_t, time_min), _push_int, _write_int },
	{ "time_min_str", offsetof(sbatch_opt_t, time_min_str), _push_string, _write_string },
	{ "partition", offsetof(sbatch_opt_t, partition), _push_string, _write_string },
	/* explicitly skip distribution and plane, may require special handling */
	{ "job_name", offsetof(sbatch_opt_t, job_name), _push_string, _write_string},
	{ "jobid", offsetof(sbatch_opt_t, jobid), _push_uint, _write_uint },
	{ "jobid_set", offsetof(sbatch_opt_t, jobid_set), _push_bool, _write_bool },
	{ "mpi_type", offsetof(sbatch_opt_t, mpi_type), _push_string, _write_string },
	{ "dependency", offsetof(sbatch_opt_t, dependency), _push_string, _write_string },
	{ "nice", offsetof(sbatch_opt_t, nice), _push_int, _write_int },
	{ "priority", offsetof(sbatch_opt_t, priority), _push_uint32_t, _write_uint32_t },
	{ "account", offsetof(sbatch_opt_t, account), _push_string, _write_string },
	{ "comment", offsetof(sbatch_opt_t, comment), _push_string, _write_string },
	{ "propagate", offsetof(sbatch_opt_t, propagate), _push_string, _write_string },
	{ "qos", offsetof(sbatch_opt_t, qos), _push_string, _write_string },
	{ "immediate", offsetof(sbatch_opt_t, immediate), _push_int, _write_int },
	{ "warn_flags", offsetof(sbatch_opt_t, warn_flags), _push_uint16_t, _write_uint16_t },
	{ "warn_signal", offsetof(sbatch_opt_t, warn_signal), _push_uint16_t, _write_uint16_t },
	{ "warn_time", offsetof(sbatch_opt_t, warn_time), _push_uint16_t, _write_uint16_t },
	{ "hold", offsetof(sbatch_opt_t, hold), _push_bool, _write_bool },
	{ "parsable", offsetof(sbatch_opt_t, parsable), _push_bool, _write_bool },
	{ "no_kill", offsetof(sbatch_opt_t, no_kill), _push_bool, _write_bool },
	{ "requeue", offsetof(sbatch_opt_t, requeue), _push_int, _write_int },
	{ "open_mode", offsetof(sbatch_opt_t, open_mode), _push_uint8_t, _write_uint8_t },
	{ "acctg_freq", offsetof(sbatch_opt_t, acctg_freq), _push_string, _write_string },
	{ "licenses", offsetof(sbatch_opt_t, licenses), _push_string, _write_string },
	{ "network", offsetof(sbatch_opt_t, network), _push_string, _write_string },
	{ "overcommit", offsetof(sbatch_opt_t, overcommit), _push_bool, _write_bool },
	{ "shared", offsetof(sbatch_opt_t, shared), _push_uint16_t, _write_uint16_t},
	{ "quiet", offsetof(sbatch_opt_t, quiet), _push_int, _write_int },
	{ "verbose", offsetof(sbatch_opt_t, verbose), _push_int, _write_int },
	{ "wait_all_nodes", offsetof(sbatch_opt_t, wait_all_nodes), _push_uint16_t, _write_uint16_t },
	{ "wrap", offsetof(sbatch_opt_t, wrap), _push_uint16_t, _write_uint16_t },

	/* constraint options */
	{ "mincpus", offsetof(sbatch_opt_t, mincpus), _push_int, _write_int },
	{ "minsockets", offsetof(sbatch_opt_t, minsockets), _push_int, _write_int },
	{ "mincores", offsetof(sbatch_opt_t, mincores), _push_int, _write_int },
	{ "minthreads", offsetof(sbatch_opt_t, minthreads), _push_int, _write_int },
	{ "mem_per_cpu", offsetof(sbatch_opt_t, mem_per_cpu), _push_int64_t, _write_int64_t },
	{ "mem", offsetof(sbatch_opt_t, realmem), _push_int64_t, _write_int64_t },
	{ "tmpdisk", offsetof(sbatch_opt_t, tmpdisk), _push_long, _write_long },
	{ "constraints", offsetof(sbatch_opt_t, constraints), _push_string, _write_string },
	{ "cluster_constraints", offsetof(sbatch_opt_t, c_constraints), _push_string, _write_string },
	{ "gres", offsetof(sbatch_opt_t, gres), _push_string, _write_string },
	{ "contiguous", offsetof(sbatch_opt_t, contiguous), _push_bool, _write_bool },
	{ "nodelist", offsetof(sbatch_opt_t, nodelist), _push_string, _write_string },
	{ "exc_nodes", offsetof(sbatch_opt_t, exc_nodes), _push_string, _write_string },
	/* only bluegene reboot option for now */
	{ "reboot", offsetof(sbatch_opt_t, reboot), _push_bool, _write_bool },
	/* remaining options */
	{ "array_inx", offsetof(sbatch_opt_t, array_inx), _push_string, _write_string },
	{ "begin", offsetof(sbatch_opt_t, begin), _push_time_t, _write_time_t },
	{ "mail_type", offsetof(sbatch_opt_t, mail_type), _push_uint16_t, _write_uint16_t },
	{ "mail_user", offsetof(sbatch_opt_t, mail_user), _push_string, _write_string },
	{ "ofname", offsetof(sbatch_opt_t, ofname), _push_string, _write_string },
	{ "ifname", offsetof(sbatch_opt_t, ifname), _push_string, _write_string },
	{ "efname", offsetof(sbatch_opt_t, efname), _push_string, _write_string },
	{ "get_user_env_time", offsetof(sbatch_opt_t, get_user_env_time), _push_int, _write_int },
	{ "get_user_env_mode", offsetof(sbatch_opt_t, get_user_env_mode), _push_int, _write_int },
	{ "export_env", offsetof(sbatch_opt_t, export_env), _push_string, _write_string },
	{ "export_file", offsetof(sbatch_opt_t, export_file), _push_string, _write_string },
	{ "wait", offsetof(sbatch_opt_t, wait), _push_bool, _write_bool },
	{ "wait_all_nodes", offsetof(sbatch_opt_t, wait_all_nodes), _push_uint16_t, _write_uint16_t },
	{ "wckey", offsetof(sbatch_opt_t, wckey), _push_string, _write_string },
	{ "reservation", offsetof(sbatch_opt_t, reservation), _push_string, _write_string },
	{ "ckpt_interval", offsetof(sbatch_opt_t, ckpt_interval), _push_int, _write_int },
	{ "ckpt_interval_str", offsetof(sbatch_opt_t, ckpt_interval_str), _push_string, _write_string },
	{ "ckpt_dir", offsetof(sbatch_opt_t, ckpt_dir), _push_string, _write_string },
	{ "req_switch", offsetof(sbatch_opt_t, req_switch), _push_int, _write_int },
	{ "wait4switch", offsetof(sbatch_opt_t, wait4switch), _push_int, _write_int },
	/* skip spank env for the moment -- TODO SOON! */
	{ "umask", offsetof(sbatch_opt_t, umask), _push_int, _write_int },
	{ "core_spec", offsetof(sbatch_opt_t, core_spec), _push_int, _write_int },
	{ "cpu_freq_min", offsetof(sbatch_opt_t, cpu_freq_min), _push_uint32_t, _write_uint32_t },
	{ "cpu_freq_max", offsetof(sbatch_opt_t, cpu_freq_max), _push_uint32_t, _write_uint32_t },
	{ "cpu_freq_gov", offsetof(sbatch_opt_t, cpu_freq_gov), _push_uint32_t, _write_uint32_t },
	{ "test_only", offsetof(sbatch_opt_t, test_only), _push_bool, _write_bool },
	{ "burst_buffer_file", offsetof(sbatch_opt_t, burst_buffer_file), _push_string, _write_string },
	{ "power_flags", offsetof(sbatch_opt_t, power_flags), _push_uint8_t, _write_uint8_t },
	{ "mcs_label", offsetof(sbatch_opt_t, mcs_label), _push_string, _write_string },
	{ "deadline", offsetof(sbatch_opt_t, deadline), _push_time_t, _write_time_t },
	{ "job_flags", offsetof(sbatch_opt_t, job_flags), _push_uint32_t, _write_uint32_t },
	{ "delay_boot", offsetof(sbatch_opt_t, delay_boot), _push_uint32_t, _write_uint32_t },
	{ NULL, 0, NULL, NULL }
};

static struct option_string srun_opt_names[] = {
	{ "progname", offsetof(srun_opt_t, progname), _push_string, NULL },
	{ "multi_prog", offsetof(srun_opt_t, multi_prog), _push_bool, NULL },
	{ "multi_prog_cmds", offsetof(srun_opt_t, multi_prog_cmds), _push_int32_t, NULL },
	{ "user",     offsetof(srun_opt_t, user), _push_string, NULL },
	{ "uid",      offsetof(srun_opt_t, uid), _push_uid, NULL },
	{ "gid",      offsetof(srun_opt_t, gid), _push_gid, NULL },
	{ "euid",     offsetof(srun_opt_t, euid), _push_uid, NULL },
	{ "egid",     offsetof(srun_opt_t, egid), _push_gid, NULL },
	{ "cwd",   offsetof(srun_opt_t, cwd), _push_string, _write_string },
	{ "cwd_set",   offsetof(srun_opt_t, cwd_set), _push_bool, _write_bool },
	{ "ntasks",   offsetof(srun_opt_t, ntasks), _push_int, _write_int },
	{ "ntasks_set", offsetof(srun_opt_t, ntasks_set), _push_bool, _write_bool },
	{ "cpus_per_task", offsetof(srun_opt_t, cpus_per_task), _push_int, _write_int },
	{ "cpus_per_task_set", offsetof(srun_opt_t, cpus_set), _push_bool, _write_bool },
	{ "max_threads", offsetof(srun_opt_t, max_threads), _push_int32_t, _write_int32_t },
	{ "min_nodes", offsetof(srun_opt_t, min_nodes), _push_int, _write_int },
	{ "max_nodes", offsetof(srun_opt_t, max_nodes), _push_int, _write_int },
	{ "nodes_set", offsetof(srun_opt_t, nodes_set), _push_bool, _write_bool },
	{ "sockets_per_node", offsetof(srun_opt_t, sockets_per_node), _push_int, _write_int },
	{ "cores_per_socket", offsetof(srun_opt_t, cores_per_socket), _push_int, _write_int },
	{ "threads_per_core", offsetof(srun_opt_t, threads_per_core), _push_int, _write_int },
	{ "ntasks_per_node", offsetof(srun_opt_t, ntasks_per_node), _push_int, _write_int },
	{ "ntasks_per_socket", offsetof(srun_opt_t, ntasks_per_socket), _push_int, _write_int },
	{ "ntasks_per_core", offsetof(srun_opt_t, ntasks_per_core), _push_int, _write_int },
	{ "ntasks_per_core_set", offsetof(srun_opt_t, ntasks_per_core_set), _push_bool, _write_bool },
	{ "hint", offsetof(srun_opt_t, hint_env), _push_string, _write_string },
	{ "hint_set", offsetof(srun_opt_t, hint_set), _push_bool, _write_bool },
	/* explicitly skipping mem_bind options for now since they are more complicated */
	{ "extra_set", offsetof(srun_opt_t, extra_set), _push_bool, _write_bool },
	{ "time_limit", offsetof(srun_opt_t, time_limit), _push_int, _write_int },
	{ "time_limit_str", offsetof(srun_opt_t, time_limit_str), _push_string, _write_string },
	{ "time_min", offsetof(srun_opt_t, time_min), _push_int, _write_int },
	{ "time_min_str", offsetof(srun_opt_t, time_min_str), _push_string, _write_string },
	{ "ckpt_interval", offsetof(srun_opt_t, ckpt_interval), _push_int, _write_int },
	{ "ckpt_interval_str", offsetof(srun_opt_t, ckpt_interval_str), _push_string, _write_string },
	{ "ckpt_dir", offsetof(srun_opt_t, ckpt_dir), _push_string, _write_string },
	{ "exclusive", offsetof(srun_opt_t, exclusive), _push_bool, _write_bool },
	{ "compress", offsetof(srun_opt_t, compress), _push_uint16_t, _write_uint16_t },
	{ "bcast_file", offsetof(srun_opt_t, bcast_file), _push_string, _write_string },
	{ "bcast_flag", offsetof(srun_opt_t, bcast_flag), _push_bool, _write_bool },
	{ "resv_port_cnt", offsetof(srun_opt_t, resv_port_cnt), _push_int, _write_int },
	{ "partition", offsetof(srun_opt_t, partition), _push_string, _write_string },
	/* explicitly skip distribution and plane, may require special handling */
	{ "cmd_name", offsetof(srun_opt_t, cmd_name), _push_string, _write_string},
	{ "job_name", offsetof(srun_opt_t, job_name), _push_string, _write_string},
	{ "job_name_set_cmd", offsetof(srun_opt_t, job_name_set_cmd), _push_bool, _write_bool },
	{ "job_name_set_env", offsetof(srun_opt_t, job_name_set_env), _push_bool, _write_bool },
	{ "jobid", offsetof(srun_opt_t, jobid), _push_uint, _write_uint },
	{ "jobid_set", offsetof(srun_opt_t, jobid_set), _push_bool, _write_bool },
	{ "dependency", offsetof(srun_opt_t, dependency), _push_string, _write_string },
	{ "nice", offsetof(srun_opt_t, nice), _push_int, _write_int },
	{ "priority", offsetof(srun_opt_t, priority), _push_uint32_t, _write_uint32_t },
	{ "account", offsetof(srun_opt_t, account), _push_string, _write_string },
	{ "comment", offsetof(srun_opt_t, comment), _push_string, _write_string },
	{ "qos", offsetof(srun_opt_t, qos), _push_string, _write_string },
	{ "ofname", offsetof(srun_opt_t, ofname), _push_string, _write_string },
	{ "ifname", offsetof(srun_opt_t, ifname), _push_string, _write_string },
	{ "efname", offsetof(srun_opt_t, efname), _push_string, _write_string },
	{ "slurmd_debug", offsetof(srun_opt_t, slurmd_debug), _push_int, _write_int },
	{ "immediate", offsetof(srun_opt_t, immediate), _push_int, _write_int },
	{ "warn_flags", offsetof(srun_opt_t, warn_flags), _push_uint16_t, _write_uint16_t },
	{ "warn_signal", offsetof(srun_opt_t, warn_signal), _push_uint16_t, _write_uint16_t },
	{ "warn_time", offsetof(srun_opt_t, warn_time), _push_uint16_t, _write_uint16_t },
	{ "hold", offsetof(srun_opt_t, hold), _push_bool, _write_bool },
	{ "hostfile", offsetof(srun_opt_t, hostfile), _push_string, _write_string },
	{ "labelio", offsetof(srun_opt_t, labelio), _push_bool, _write_bool },
	{ "unbuffered", offsetof(srun_opt_t, unbuffered), _push_bool, _write_bool },
	{ "allocate", offsetof(srun_opt_t, allocate), _push_bool, _write_bool },
	{ "noshell", offsetof(srun_opt_t, noshell), _push_bool, _write_bool },
	{ "overcommit", offsetof(srun_opt_t, overcommit), _push_bool, _write_bool },
	{ "no_kill", offsetof(srun_opt_t, no_kill), _push_bool, _write_bool },
	{ "kill_bad_exit", offsetof(srun_opt_t, kill_bad_exit), _push_int32_t, _write_int32_t },
	{ "shared", offsetof(srun_opt_t, shared), _push_uint16_t, _write_uint16_t},
	{ "max_wait", offsetof(srun_opt_t, max_wait), _push_int, _write_int },
	{ "quit_on_intr", offsetof(srun_opt_t, quit_on_intr), _push_bool, _write_bool },
	{ "disable_status", offsetof(srun_opt_t, disable_status), _push_bool, _write_bool },
	{ "quiet", offsetof(srun_opt_t, quiet), _push_int, _write_int },
	{ "parallel_debug", offsetof(srun_opt_t, parallel_debug), _push_bool, _write_bool },
	{ "debugger_test", offsetof(srun_opt_t, debugger_test), _push_bool, _write_bool },
	{ "test_only", offsetof(srun_opt_t, test_only), _push_bool, _write_bool },
	{ "profile", offsetof(srun_opt_t, profile), _push_uint32_t, _write_uint32_t },
	{ "propagate", offsetof(srun_opt_t, propagate), _push_string, _write_string },
	{ "task_epilog", offsetof(srun_opt_t, task_epilog), _push_string, _write_string },
	{ "task_prolog", offsetof(srun_opt_t, task_prolog), _push_string, _write_string },
	{ "licenses", offsetof(srun_opt_t, licenses), _push_string, _write_string },
	{ "preserve_env", offsetof(srun_opt_t, preserve_env), _push_bool, _write_bool },
	{ "export_env", offsetof(srun_opt_t, export_env), _push_string, _write_string },

	/* constraint options */
	{ "mincpus", offsetof(srun_opt_t, pn_min_cpus), _push_int32_t, _write_int32_t },
	{ "mem", offsetof(srun_opt_t, pn_min_memory), _push_int64_t, _write_int64_t },
	{ "mem_per_cpu", offsetof(srun_opt_t, mem_per_cpu), _push_int64_t, _write_int64_t },
	{ "tmpdisk", offsetof(srun_opt_t, pn_min_tmp_disk), _push_long, _write_long },
	{ "constraints", offsetof(srun_opt_t, constraints), _push_string, _write_string },
	{ "cluster_constraints", offsetof(srun_opt_t, c_constraints), _push_string, _write_string },
	{ "gres", offsetof(srun_opt_t, gres), _push_string, _write_string },
	{ "contiguous", offsetof(srun_opt_t, contiguous), _push_bool, _write_bool },
	{ "nodelist", offsetof(srun_opt_t, nodelist), _push_string, _write_string },
	{ "alloc_nodelist", offsetof(srun_opt_t, alloc_nodelist), _push_string, NULL},
	{ "exc_nodes", offsetof(srun_opt_t, exc_nodes), _push_string, _write_string },
	{ "relative", offsetof(srun_opt_t, relative), _push_int, _write_int },
	{ "relative_set", offsetof(srun_opt_t, relative_set), _push_bool, _write_bool },
	{ "max_launch_time", offsetof(srun_opt_t, max_launch_time), _push_int, _write_int },
	{ "max_exit_timeout", offsetof(srun_opt_t, max_exit_timeout), _push_int, _write_int },
	{ "msg_timeout", offsetof(srun_opt_t, msg_timeout), _push_int, _write_int },
	{ "launch_cmd", offsetof(srun_opt_t, launch_cmd), _push_bool, _write_bool },
	{ "launcher_opts", offsetof(srun_opt_t, launcher_opts), _push_string, _write_string },
	{ "network", offsetof(srun_opt_t, network), _push_string, _write_string },
	{ "network_set_env", offsetof(srun_opt_t, network_set_env), _push_bool, _write_bool },
	/* only bluegene reboot option for now */
	{ "reboot", offsetof(srun_opt_t, reboot), _push_bool, _write_bool },
	/* remaining options */
	{ "prolog", offsetof(srun_opt_t, prolog), _push_string, _write_string },
	{ "epilog", offsetof(srun_opt_t, epilog), _push_string, _write_string },
	{ "begin", offsetof(srun_opt_t, begin), _push_time_t, _write_time_t },
	{ "mail_type", offsetof(srun_opt_t, mail_type), _push_uint16_t, _write_uint16_t },
	{ "mail_user", offsetof(srun_opt_t, mail_user), _push_string, _write_string },
	{ "open_mode", offsetof(srun_opt_t, open_mode), _push_uint8_t, _write_uint8_t },
	{ "acctg_freq", offsetof(srun_opt_t, acctg_freq), _push_string, _write_string },
	{ "pty", offsetof(srun_opt_t, pty), _push_bool, _write_bool },
	{ "restart_dir", offsetof(srun_opt_t, restart_dir), _push_string, _write_string },
	{ "argc", offsetof(srun_opt_t, argc), _push_int, _write_int },
	{ "argv", offsetof(srun_opt_t, argv), _push_stringarray, NULL },
	{ "wckey", offsetof(sbatch_opt_t, wckey), _push_string, _write_string },
	{ "reservation", offsetof(srun_opt_t, reservation), _push_string, _write_string },
	{ "req_switch", offsetof(srun_opt_t, req_switch), _push_int, _write_int },
	{ "wait4switch", offsetof(srun_opt_t, wait4switch), _push_int, _write_int },
	/* skip spank env for the moment -- TODO SOON! */
	{ "user_managed_io", offsetof(srun_opt_t, user_managed_io), _push_bool, _write_bool },
	{ "core_spec", offsetof(srun_opt_t, core_spec), _push_int, _write_int },
	{ "core_spec_set", offsetof(srun_opt_t, core_spec_set), _push_bool, _write_bool },
	{ "burst_buffer", offsetof(srun_opt_t, burst_buffer), _push_string, _write_string },
	{ "cpu_freq_min", offsetof(srun_opt_t, cpu_freq_min), _push_uint32_t, _write_uint32_t },
	{ "cpu_freq_max", offsetof(srun_opt_t, cpu_freq_max), _push_uint32_t, _write_uint32_t },
	{ "cpu_freq_gov", offsetof(srun_opt_t, cpu_freq_gov), _push_uint32_t, _write_uint32_t },
	{ "power_flags", offsetof(srun_opt_t, power_flags), _push_uint8_t, _write_uint8_t },
	{ "mcs_label", offsetof(srun_opt_t, mcs_label), _push_string, _write_string },
	{ "deadline", offsetof(srun_opt_t, deadline), _push_time_t, _write_time_t },
	{ "job_flags", offsetof(srun_opt_t, job_flags), _push_uint32_t, _write_uint32_t },
	{ "delay_boot", offsetof(srun_opt_t, delay_boot), _push_uint32_t, _write_uint32_t },
	{ "pack_group", offsetof(srun_opt_t, pack_group), _push_string, _write_string },
	{ "pack_step_cnt", offsetof(srun_opt_t, pack_step_cnt), _push_int, _write_int },
	{ NULL, 0, NULL, NULL }
};

/*
 *  NOTE: The init callback should never be called multiple times,
 *   let alone called from multiple threads. Therefore, locking
 *   is unnecessary here.
 */
int init(void)
{
        int rc = SLURM_SUCCESS;

        /*
         * Need to dlopen() the Lua library to ensure plugins see
         * appropriate symptoms
         */
        if ((rc = xlua_dlopen()) != SLURM_SUCCESS)
                return rc;

        return _load_script();
}

int fini(void)
{
        lua_close (L);
        return SLURM_SUCCESS;
}


/* in the future this function and the option_string data structures should
 * probably be converted to use a binary search or other faster method for
 * discovering the requested option */
const struct option_string *find_opt_str(const char *name,
                                         const struct option_string *opt_str)
{
	size_t idx = 0;
	for (idx = 0; opt_str[idx].name; idx++)
		if (!xstrcmp(opt_str[idx].name, name)) {
			return &(opt_str[idx]);
		}
	return NULL;
}

static bool _push_string(void *data, const char *name,
			    const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	char **tgt = (char **) (data + opt_str->offset);
	lua_pushstring(L, *tgt);
	return true;
}

static bool _write_string(void *data, int idx, const char *name,
			    const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	char **tgt = (char **) (data + opt_str->offset);
	char *str = luaL_checkstring(L, idx);
	xfree(*tgt);
	if (str)
		*tgt = xstrdup(str);
	return true;
}

static bool _push_bool(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	bool *tgt = (bool *) (data + opt_str->offset);
	lua_pushboolean(L, (int) *tgt);
	return true;
}

static bool _write_bool(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	return false;
}

static bool _push_long(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	long *tgt = (long *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_long(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	long *tgt = (long *) (data + opt_str->offset);
	long towrite = (long) luaL_checknumber(L, idx);
	*tgt = towrite;
	return true;
}

static bool _push_int(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	int *tgt = (int *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_int(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	int *tgt = (int *) (data + opt_str->offset);
	int towrite = (int) luaL_checknumber(L, idx);
	*tgt = towrite;
	return true;
}

static bool _push_uint(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	unsigned int *tgt = (unsigned int *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_uint(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	unsigned int *tgt = (unsigned int *) (data + opt_str->offset);
	unsigned int towrite = (unsigned int) luaL_checknumber(L, idx);
	*tgt = towrite;
	return true;
}

static bool _push_int64_t(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	int64_t *tgt = (int64_t *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_int64_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	int64_t *tgt = (int64_t *) (data + opt_str->offset);
	int64_t towrite = (int64_t) luaL_checknumber(L, idx);
	*tgt = towrite;
	return true;
}

static bool _push_int32_t(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	int32_t *tgt = (int32_t *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_int32_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	return false;
}

static bool _push_uint64_t(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	uint64_t *tgt = (uint64_t *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_uint64_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	uint64_t *tgt = (uint64_t *) (data + opt_str->offset);
	uint64_t towrite = (uint64_t) luaL_checknumber(L, idx);
	*tgt = towrite;
	return true;
}

static bool _push_uint32_t(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	uint32_t *tgt = (uint32_t *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_uint32_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	uint32_t *tgt = (uint32_t *) (data + opt_str->offset);
	uint32_t towrite = (uint32_t) luaL_checknumber(L, idx);
	*tgt = towrite;
	return true;
}

static bool _push_uint16_t(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	uint16_t *tgt = (uint16_t *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_uint16_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	uint16_t *tgt = (uint16_t *) (data + opt_str->offset);
	uint16_t towrite = (uint16_t) luaL_checknumber(L, idx);
	*tgt = towrite;
	return true;
}

static bool _push_uint8_t(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	uint8_t *tgt = (uint8_t *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_uint8_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	uint8_t *tgt = (uint8_t *) (data + opt_str->offset);
	uint8_t towrite = (uint8_t) luaL_checknumber(L, idx);
	*tgt = towrite;
	return true;
}

static bool _push_time_t(void *data, const char *name,
			 const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	time_t *tgt = (time_t *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_time_t(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;

	time_t *tgt = (time_t *) (data + opt_str->offset);
	time_t towrite = (time_t) luaL_checknumber(L, idx);
	*tgt = towrite;
	return true;
}

static bool _push_uid(void *data, const char *name,
			   const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	uid_t *tgt = (uid_t *) (data + opt_str->offset);
	lua_pushnumber(L, (double) *tgt);
	return true;
}

static bool _write_uid(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	return false;
}

static bool _push_gid(void *data, const char *name,
			   const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	gid_t *tgt = (gid_t *) (data + opt_str->offset);
	lua_pushnumber(L, *tgt);
	return true;
}

static bool _write_gid(void *data, int idx, const char *name,
			const struct option_string *opt_str, lua_State *L)
{
	return false;
}


static int _stringarray_field_index(lua_State *L)
{
	char **strarray = NULL;
	int strarray_sz = 0;
	int idx = luaL_checkint(L, 2);
	lua_getmetatable(L, -2);
	lua_getfield(L, -1, "_stringarray");
	strarray = lua_touserdata(L, -1);
	lua_getfield(L, -2, "_stringarray_sz");
	strarray_sz = luaL_checkint(L, -1);

	/* lua indexing starts at 1 */
	if (idx > 0 && idx <= strarray_sz) {
		lua_pushstring(L, strarray[idx - 1]);
	} else {
		lua_pushnil(L);
	}
	return 1;
}

static bool _push_stringarray(void *data, const char *name,
			   const struct option_string *opt_str, lua_State *L)
{
	if (!data || !name || !opt_str || !L)
		return false;
	char ***tgt = (char ***) (data + opt_str->offset);
	char **ptr = *tgt;
	int sz = 0;
	for ( ptr = *tgt; ptr && *ptr; ptr++) {
		sz++;
	}

	lua_newtable(L);
	lua_newtable(L);
	lua_pushcfunction(L, _stringarray_field_index);
	lua_setfield(L, -2, "__index");
/* TODO allow plugin to extend string array
	lua_pushcfunction(L, _stringarray_field);
	lua_setfield(L, -2, "__newindex");
*/
	lua_pushlightuserdata(L, *tgt);
	lua_setfield(L, -2, "_stringarray");
	lua_pushnumber(L, (double) sz);
	lua_setfield(L, -2, "_stringarray_sz");
	lua_setmetatable(L, -2);

	return true;
}

static int _get_option_field_index(lua_State *L)
{
	const char *name;
	const struct option_string *opt_str;
	const struct option_string *req_option = NULL;
	void *data = NULL;

	name = luaL_checkstring(L, 2);
	lua_getmetatable(L, -2);
	lua_getfield(L, -1, "_opt_str");
	opt_str = lua_touserdata(L, -1);
	lua_getfield(L, -2, "_opt_data");
	data = lua_touserdata(L, -1);

	req_option = find_opt_str(name, opt_str);
	if ((req_option->read)(data, name, req_option, L))
		return 1;

	return 0;
}

static int _set_option_field(lua_State *L)
{
	const char *name = NULL;
	const struct option_string *opt_str;
	const struct option_string *req_option = NULL;
	void *data = NULL;

	name = luaL_checkstring(L, 2);
	lua_getmetatable(L, -3);
	lua_getfield(L, -1, "_opt_str");
	opt_str = lua_touserdata(L, -1);
	lua_getfield(L, -2, "_opt_data");
	data = lua_touserdata(L, -1);

	req_option = find_opt_str(name, opt_str);
	if (!(req_option->write))
		return 0;
	if ((req_option->write)(data, 3, name, req_option, L))
		return 1;

	return 0;
}

static void _push_options(int cli_type, void *opt_data)
{
	struct option_string *opt_str = NULL;
	if (cli_type == CLI_SALLOC) {
		opt_str = salloc_opt_names;
	} else if (cli_type == CLI_SBATCH) {
		opt_str = sbatch_opt_names;
		sbatch_opt_t *sb = (sbatch_opt_t *) opt_data;
	} else if (cli_type == CLI_SRUN) {
		opt_str = srun_opt_names;
	}

	lua_newtable(L);

	lua_newtable(L);
	lua_pushcfunction(L, _get_option_field_index);
	lua_setfield(L, -2, "__index");
	lua_pushcfunction(L, _set_option_field);
	lua_setfield(L, -2, "__newindex");
	/* Store the job descriptor in the metatable, so the index
	 * function knows which struct it's getting data for.
	 */
	lua_pushlightuserdata(L, opt_str);
	lua_setfield(L, -2, "_opt_str");
	lua_pushlightuserdata(L, opt_data);
	lua_setfield(L, -2, "_opt_data");
	lua_setmetatable(L, -2);
}

static int _log_lua_error (lua_State *L)
{
	const char *prefix  = "cli_filter/lua";
	const char *msg     = lua_tostring (L, -1);
	error ("%s: %s", prefix, msg);
	return (0);
}

static int _log_lua_user_msg (lua_State *L)
{
	const char *msg = lua_tostring(L, -1);

	xfree(user_msg);
	user_msg = xstrdup(msg);
	return (0);
}


/*
 *  Lua interface to SLURM log facility:
 */
static int _log_lua_msg (lua_State *L)
{
	const char *prefix  = "cli_filter/lua";
	int        level    = 0;
	const char *msg;

	/*
	 *  Optional numeric prefix indicating the log level
	 *  of the message.
	 */

	/*
	 *  Pop message off the lua stack
	 */
	msg = lua_tostring(L, -1);
	lua_pop (L, 1);
	/*
	 *  Pop level off stack:
	 */
	level = (int)lua_tonumber (L, -1);
	lua_pop (L, 1);

	/*
	 *  Call appropriate slurm log function based on log-level argument
	 */
	if (level > 4)
		debug4 ("%s: %s", prefix, msg);
	else if (level == 4)
		debug3 ("%s: %s", prefix, msg);
	else if (level == 3)
		debug2 ("%s: %s", prefix, msg);
	else if (level == 2)
		debug ("%s: %s", prefix, msg);
	else if (level == 1)
		verbose ("%s: %s", prefix, msg);
	else if (level == 0)
		info ("%s: %s", prefix, msg);
	return (0);
}


/*
 *  check that global symbol [name] in lua script is a function
 */
static int _check_lua_script_function(const char *name)
{
	int rc = 0;
	lua_getglobal(L, name);
	if (!lua_isfunction(L, -1))
		rc = -1;
	lua_pop(L, -1);
	return (rc);
}

/*
 *   Verify all required functions are defined in the job_submit/lua script
 */
static int _check_lua_script_functions(void)
{
	int rc = 0;
	int i;
	const char *fns[] = {
		"slurm_cli_setup_defaults",
		"slurm_cli_pre_submit",
		NULL
	};

	i = 0;
	do {
		if (_check_lua_script_function(fns[i]) < 0) {
			error("cli_filter/lua: %s: "
			      "missing required function %s",
			      lua_script_path, fns[i]);
			rc = -1;
		}
	} while (fns[++i]);

	return (rc);
}

static void _lua_table_register(lua_State *L, const char *libname,
				const luaL_Reg *l)
{
#if LUA_VERSION_NUM == 501
	luaL_register(L, libname, l);
#else
	luaL_setfuncs(L, l, 0);
	if (libname)
		lua_setglobal(L, libname);
#endif
}

static void _register_lua_slurm_output_functions (void)
{
	/*
	 *  Register slurm output functions in a global "slurm" table
	 */
	lua_newtable (L);
	_lua_table_register(L, NULL, slurm_functions);

	/*
	 *  Create more user-friendly lua versions of SLURM log functions.
	 */
	luaL_loadstring (L, "slurm.error (string.format(unpack({...})))");
	lua_setfield (L, -2, "log_error");
	luaL_loadstring (L, "slurm.log (0, string.format(unpack({...})))");
	lua_setfield (L, -2, "log_info");
	luaL_loadstring (L, "slurm.log (1, string.format(unpack({...})))");
	lua_setfield (L, -2, "log_verbose");
	luaL_loadstring (L, "slurm.log (2, string.format(unpack({...})))");
	lua_setfield (L, -2, "log_debug");
	luaL_loadstring (L, "slurm.log (3, string.format(unpack({...})))");
	lua_setfield (L, -2, "log_debug2");
	luaL_loadstring (L, "slurm.log (4, string.format(unpack({...})))");
	lua_setfield (L, -2, "log_debug3");
	luaL_loadstring (L, "slurm.log (5, string.format(unpack({...})))");
	lua_setfield (L, -2, "log_debug4");
	luaL_loadstring (L, "slurm.user_msg (string.format(unpack({...})))");
	lua_setfield (L, -2, "log_user");

	/*
	 * Error codes: slurm.SUCCESS, slurm.FAILURE, slurm.ERROR, etc.
	 */
	lua_pushnumber (L, SLURM_FAILURE);
	lua_setfield (L, -2, "FAILURE");
	lua_pushnumber (L, SLURM_ERROR);
	lua_setfield (L, -2, "ERROR");
	lua_pushnumber (L, SLURM_SUCCESS);
	lua_setfield (L, -2, "SUCCESS");
	lua_pushnumber (L, ESLURM_INVALID_LICENSES);
	lua_setfield (L, -2, "ESLURM_INVALID_LICENSES");

	/*
	 * Other definitions needed to interpret data
	 * slurm.MEM_PER_CPU, slurm.NO_VAL, etc.
	 */
	lua_pushnumber (L, ALLOC_SID_ADMIN_HOLD);
	lua_setfield (L, -2, "ALLOC_SID_ADMIN_HOLD");
	lua_pushnumber (L, ALLOC_SID_USER_HOLD);
	lua_setfield (L, -2, "ALLOC_SID_USER_HOLD");
	lua_pushnumber (L, INFINITE);
	lua_setfield (L, -2, "INFINITE");
	lua_pushnumber (L, INFINITE64);
	lua_setfield (L, -2, "INFINITE64");
	lua_pushnumber (L, MAIL_JOB_BEGIN);
	lua_setfield (L, -2, "MAIL_JOB_BEGIN");
	lua_pushnumber (L, MAIL_JOB_END);
	lua_setfield (L, -2, "MAIL_JOB_END");
	lua_pushnumber (L, MAIL_JOB_FAIL);
	lua_setfield (L, -2, "MAIL_JOB_FAIL");
	lua_pushnumber (L, MAIL_JOB_REQUEUE);
	lua_setfield (L, -2, "MAIL_JOB_REQUEUE");
	lua_pushnumber (L, MAIL_JOB_TIME100);
	lua_setfield (L, -2, "MAIL_JOB_TIME100");
	lua_pushnumber (L, MAIL_JOB_TIME90);
	lua_setfield (L, -2, "MAIL_JOB_TIME890");
	lua_pushnumber (L, MAIL_JOB_TIME80);
	lua_setfield (L, -2, "MAIL_JOB_TIME80");
	lua_pushnumber (L, MAIL_JOB_TIME50);
	lua_setfield (L, -2, "MAIL_JOB_TIME50");
	lua_pushnumber (L, MAIL_JOB_STAGE_OUT);
	lua_setfield (L, -2, "MAIL_JOB_STAGE_OUT");
	lua_pushnumber (L, MEM_PER_CPU);
	lua_setfield (L, -2, "MEM_PER_CPU");
	lua_pushnumber (L, NICE_OFFSET);
	lua_setfield (L, -2, "NICE_OFFSET");
	lua_pushnumber (L, JOB_SHARED_NONE);
	lua_setfield (L, -2, "JOB_SHARED_NONE");
	lua_pushnumber (L, JOB_SHARED_OK);
	lua_setfield (L, -2, "JOB_SHARED_OK");
	lua_pushnumber (L, JOB_SHARED_USER);
	lua_setfield (L, -2, "JOB_SHARED_USER");
	lua_pushnumber (L, JOB_SHARED_MCS);
	lua_setfield (L, -2, "JOB_SHARED_MCS");
	lua_pushnumber (L, NO_VAL64);
	lua_setfield (L, -2, "NO_VAL64");
	lua_pushnumber (L, NO_VAL);
	lua_setfield (L, -2, "NO_VAL");
	lua_pushnumber (L, (uint16_t) NO_VAL);
	lua_setfield (L, -2, "NO_VAL16");
	lua_pushnumber (L, (uint8_t) NO_VAL);
	lua_setfield (L, -2, "NO_VAL8");

	/*
	 * job_desc bitflags
	 */
	lua_pushnumber (L, GRES_ENFORCE_BIND);
	lua_setfield (L, -2, "GRES_ENFORCE_BIND");
	lua_pushnumber (L, KILL_INV_DEP);
	lua_setfield (L, -2, "KILL_INV_DEP");
	lua_pushnumber (L, NO_KILL_INV_DEP);
	lua_setfield (L, -2, "NO_KILL_INV_DEP");
	lua_pushnumber (L, SPREAD_JOB);
	lua_setfield (L, -2, "SPREAD_JOB");
	lua_pushnumber (L, USE_MIN_NODES);
	lua_setfield (L, -2, "USE_MIN_NODES");
	lua_pushnumber(L, CLI_SALLOC);
	lua_setfield(L, -2, "CLI_SALLOC");
	lua_pushnumber(L, CLI_SBATCH);
	lua_setfield(L, -2, "CLI_SBATCH");
	lua_pushnumber(L, CLI_SRUN);
	lua_setfield(L, -2, "CLI_SRUN");

	lua_setglobal (L, "slurm");
}

static void _register_lua_slurm_struct_functions (void)
{
#if 0
	lua_pushcfunction(L, _get_job_env_field_name);
	lua_setglobal(L, "_get_job_env_field_name");
	lua_pushcfunction(L, _get_job_req_field_name);
	lua_setglobal(L, "_get_job_req_field_name");
	lua_pushcfunction(L, _set_job_env_field);
	lua_setglobal(L, "_set_job_env_field");
	lua_pushcfunction(L, _set_job_req_field);
	lua_setglobal(L, "_set_job_req_field");
	lua_pushcfunction(L, _get_part_rec_field);
	lua_setglobal(L, "_get_part_rec_field");
#endif
}

static int _load_script(void)
{
	int rc = SLURM_SUCCESS;
	struct stat st;

	/*
	 * Need to dlopen() the Lua library to ensure plugins see
	 * appropriate symptoms
	 */
	if ((rc = xlua_dlopen()) != SLURM_SUCCESS)
		return rc;


	if (stat(lua_script_path, &st) != 0) {
		return error("Unable to stat %s: %s",
		             lua_script_path, strerror(errno));
	}

	/*
	 *  Initilize lua
	 */
	L = luaL_newstate();
	luaL_openlibs(L);
	if (luaL_loadfile(L, lua_script_path)) {
		rc = error("lua: %s: %s", lua_script_path,
		           lua_tostring(L, -1));
		lua_pop(L, 1);
		return rc;
	}

	/*
	 *  Register SLURM functions in lua state:
	 *  logging and slurm structure read/write functions
	 */
	_register_lua_slurm_output_functions();
	_register_lua_slurm_struct_functions();

	/*
	 *  Run the user script:
	 */
	if (lua_pcall(L, 0, 1, 0) != 0) {
		rc = error("cli_filter/lua: %s: %s",
		           lua_script_path, lua_tostring(L, -1));
		lua_pop(L, 1);
		return rc;
	}

	/*
	 *  Get any return code from the lua script
	 */
	rc = (int) lua_tonumber(L, -1);
	if (rc != SLURM_SUCCESS) {
		(void) error("cli_filter/lua: %s: returned %d on load",
		             lua_script_path, rc);
		lua_pop (L, 1);
		return rc;
	}

	/*
	 *  Check for required lua script functions:
	 */
	rc = _check_lua_script_functions();
	if (rc != SLURM_SUCCESS) {
		return rc;
	}

	return SLURM_SUCCESS;
}

static void _stack_dump (char *header, lua_State *L)
{
#if _DEBUG_LUA
	int i;
	int top = lua_gettop(L);

	info("%s: dumping cli_filter/lua stack, %d elements", header, top);
	for (i = 1; i <= top; i++) {  /* repeat for each level */
		int type = lua_type(L, i);
		switch (type) {
			case LUA_TSTRING:
				info("string[%d]:%s", i, lua_tostring(L, i));
				break;
			case LUA_TBOOLEAN:
				info("boolean[%d]:%s", i,
				     lua_toboolean(L, i) ? "true" : "false");
				break;
			case LUA_TNUMBER:
				info("number[%d]:%d", i,
				     (int) lua_tonumber(L, i));
				break;
			default:
				info("other[%d]:%s", i, lua_typename(L, type));
				break;
		}
	}
#endif
}

extern int setup_defaults(int cli_type, void *opt, char **err_msg) {
	int rc = SLURM_ERROR;
	(void) _load_script();

	lua_getglobal(L, "slurm_cli_setup_defaults");
	if (lua_isnil(L, -1))
		goto out;
	lua_pushnumber(L, (double) cli_type);
	_push_options(cli_type, opt);
	if (lua_pcall(L, 2, 1, 0) != 0) {
		error("%s/lua: %s: %s", __func__, lua_script_path,
			lua_tostring(L, -1));
	} else {
		if (lua_isnumber(L, -1)) {
			rc = lua_tonumber(L, -1);
		} else {
			info("%s/lua: %s: non-numeric return code", __func__,
				lua_script_path);
			rc = SLURM_SUCCESS;
		}
		lua_pop(L, 1);
	}
	if (user_msg) {
		if (err_msg) {
			*err_msg = user_msg;
			user_msg = NULL;
		} else
			xfree(user_msg);
	}

out:	lua_close (L);
	return rc;
}

extern int pre_submit(int cli_type, void *opt, char **err_msg) {
	int rc = SLURM_ERROR;

	(void) _load_script();

	/*
	 *  All lua script functions should have been verified during
	 *   initialization:
	 */
	lua_getglobal(L, "slurm_cli_pre_submit");
	if (lua_isnil(L, -1))
		goto out;

	lua_pushnumber(L, (double) cli_type);
	_push_options(cli_type, opt);

	_stack_dump("cli_filter, before lua_pcall", L);
	if (lua_pcall (L, 2, 1, 0) != 0) {
		error("%s/lua: %s: %s",
		      __func__, lua_script_path, lua_tostring (L, -1));
	} else {
		if (lua_isnumber(L, -1)) {
			rc = lua_tonumber(L, -1);
		} else {
			info("%s/lua: %s: non-numeric return code",
			      __func__, lua_script_path);
			rc = SLURM_SUCCESS;
		}
		lua_pop(L, 1);
	}
	_stack_dump("cli_filter, after lua_pcall", L);
	if (user_msg) {
		if (err_msg) {
			*err_msg = user_msg;
			user_msg = NULL;
		} else
			xfree(user_msg);
	}

out:	lua_close (L);
	return rc;
}

extern int post_submit(int cli_type, uint32_t jobid, void *opt, char **err_msg) {
	return SLURM_SUCCESS;
}
