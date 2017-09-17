/*****************************************************************************\
 *  cli_filter_user_defaults.c - Set defaults CLI option processing specifications.
 *****************************************************************************
 *  Copyright (C) 2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette1@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
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
#include <unistd.h>

#include "slurm/slurm_errno.h"
#include "src/common/slurm_xlator.h"
#include "src/common/uid.h"
#include "src/salloc/salloc_opt.h"
#include "src/sbatch/sbatch_opt.h"
#include "src/srun/libsrun/srun_opt.h"

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
const char plugin_name[]       	= "cli filter user defaults plugin";
const char plugin_type[]       	= "cli_filter/user_defaults";
const uint32_t plugin_version   = SLURM_VERSION_NUMBER;

/*****************************************************************************\
 * We've provided a simple example of the type of things you can do with this
 * plugin. If you develop another plugin that may be of interest to others
 * please post it to slurm-dev@schedmd.com  Thanks!
\*****************************************************************************/
extern int setup_defaults(int cli_type, void *opt, char **err_mesg) {
	struct passwd pwd, *result;
	char buffer[PW_BUF_SIZE];
	char defaults_path[PATH_MAX];
	FILE *fp = NULL;
	int rc = 0;

	rc = slurm_getpwuid_r(getuid(), &pwd, buffer, PW_BUF_SIZE, &result);
	if (!result || (rc != 0)) {
		error("Failed to lookup user homedir to load slurm defaults.");
		return SLURM_SUCCESS;
	}
	snprintf(defaults_path, sizeof(defaults_path), "%s/.slurm_defaults",
		 result->pw_dir);
	fp = fopen(defaults_path, "r");

	if (!fp) {
		/* $HOME/.slurm_defaults does not exist or is not readable
		 * will assume user wants stock defaults */
		return SLURM_SUCCESS;
	}

	/* parse user $HOME/.slurm_defaults file here and populate opt data
	 * structure with user-selected defaults */
	
	return SLURM_SUCCESS;
}
extern int pre_submit(int cli_type, void *opt, char **err_mesg) {
	return SLURM_SUCCESS;
}

extern int post_submit(int cli_type, uint32_t jobid, void *opt, char **err_mesg) {
	return SLURM_SUCCESS;
}
