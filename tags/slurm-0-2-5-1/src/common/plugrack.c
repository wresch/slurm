/*****************************************************************************\
 * plugrack.c - an intelligent container for plugins
 *****************************************************************************
 *  Copyright (C) 2002 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Jay Windley <jwindley@lnxi.com>.
 *  UCRL-CODE-2002-040.
 *  
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://www.llnl.gov/linux/slurm/>.
 *  
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *  
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *  
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.
\*****************************************************************************/

#if HAVE_CONFIG_H
#  include "config.h"

#  if HAVE_DIRENT_H
#    include <dirent.h>
#    define NAMLEN(dirent) strlen((dirent)->d_name)
#  else /* ! HAVE_DIRENT_H */
#    define dirent direct
#    define NAMLEN(dirent) (dirent)->d_namlen
#  endif /* HAVE_DIRENT_H */

#  if STDC_HEADERS
#    include <string.h>
#    include <stdlib.h>
#  else /* ! STDC_HEADERS */
#    if !HAVE_STRCHR
#      define strchr index
#      define strrchr rindex
       char *strchr(), *strrchr();
#    endif /* HAVE_STRCHR */
#  endif /* STDC_HEADERS */

#  if HAVE_UNISTD_H
#    include <unistd.h>
#  endif /* HAVE_UNISTD_H */
#  if HAVE_SYS_TYPES_H
#    include <sys/types.h>
#  endif
#  if HAVE_SYS_STAT_H
#    include <sys/stat.h>
#  endif

#else /* ! HAVE_CONFIG_H */
#  include <dirent.h>
#  include <string.h>
#  include <stdlib.h>
#  include <unistd.h>
#  include <dirent.h>
#  include <sys/types.h>
#  include <sys/stat.h>
#endif /* HAVE_CONFIG_H */

#include "src/common/xassert.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/plugrack.h"

/*
 * Represents a plugin in the rack.
 *
 * full_type is the fully-qualified plugin type, e.g., "auth/kerberos".
 * For the low-level plugin interface the type can be whatever it needs
 * to be.  For the rack-level interface, the type exported by the plugin
 * must be of the form "<major>/<minor>".
 *
 * fq_path is the fully-qualified pathname to the plugin.
 *
 * plug is the plugin handle.  If it is equal to PLUGIN_INVALID_HANDLE
 * then the plugin is not currently loaded in memory.
 *
 * refcount shows how many clients have requested to use the plugin.
 * If this is zero, the rack code may decide to unload the plugin.
 */
typedef struct _plugrack_entry {
        const char *full_type;
        const char *fq_path;
        plugin_handle_t        plug;
        int refcount;
} plugrack_entry_t;

/*
 * Implementation of the plugin rack.
 *
 * entries is the list of plugrack_entry_t.
 *
 * uid is the Linux UID of the person authorized to own the plugin
 * and write to the plugin file and the directory where it is stored.
 * This field is used only if paranoia is nonzero.
 *
 * paranoia is a set of bit flags indicating what operations should be
 * done to verify the integrity and authority of the plugin before
 * loading it.
 */
struct _plugrack {
        List entries;
        const char *major_type;
        uid_t uid;
        uint8_t     paranoia;
};

#define PLUGRACK_UID_NOBODY                99        /* RedHat's, anyway. */

/*
 * Destructor function for the List code.  This should entirely
 * clean up a plugin_entry_t.
 */
static void
plugrack_entry_destructor( void *v )
{
        plugrack_entry_t *victim = v;
  
        if ( victim == NULL ) return;

        /*
         * Free memory and unload the plugin if necessary.  The assert
         * is to make sure we were actually called from the List destructor
         * which should only be callable from plugrack_destroy().
         */
        xassert( victim->refcount == 0 );
        if ( victim->full_type ) xfree( victim->full_type );
        if ( victim->fq_path ) xfree( victim->fq_path );
        if ( victim->plug != PLUGIN_INVALID_HANDLE ) 
		plugin_unload( victim->plug );
        xfree( victim );
}

/*
 * Check a pathname to see if it is owned and writable by the appropriate
 * users, and writable by no one else.  The path can be either to a file
 * or to a directory.  This is so, when fishing for plugins in a whole
 * directory, we can test the directory once and then each file.
 *
 * Returns non-zero if the file system node indicated by the path name
 * is owned by the user in the plugin rack and not writable by anyone
 * else, and these actions are requested by the rack's paranoia policy.
 */
static int
accept_path_paranoia( plugrack_t rack,
                      const char *fq_path,
                      int check_own,
                      int check_write )
{
        struct stat st;

        /* Internal function, so assert rather than fail gracefully. */
        xassert( rack );
        xassert( fq_path );
  
        if ( stat( fq_path, &st ) < 0 ) {
                return 0;
        }
  
        /* Is path owned by authorized user? */
        if ( check_own ) {
                if ( st.st_uid != rack->uid ) return 0;
        }

        /* Is path writable by others? */
        if ( check_write ) {
                if (  ( st.st_mode & S_IWGRP ) 
		   || ( st.st_mode & S_IWOTH ) ) 
			return 0;
        }

        return 1;
}


/*
 * Check a file to see if its permissions and location are appropriate
 * for a plugin.  This checks both a file and its parent directory for
 * correct ownership and writability.
 *
 * The permissions and ownerships to check are given in the paranoia
 * policy of the plugin rack.
 *
 * Returns nonzero if the plugin is acceptable.
 */
static int
accept_paranoia( plugrack_t rack, const char *fq_path )
{
        char *local;
        char *p;
  
        xassert( rack );
        xassert( fq_path );

        /* Trivial accept. */
        if ( ! rack->paranoia ) return 1;
  
        /* Make a local copy of the path name so we can write into it. */
        local = alloca( strlen( fq_path ) + 1 );
        strcpy( local, fq_path );

        if ( ! accept_path_paranoia( rack,
                                     local,
                                     rack->paranoia & 
				     PLUGRACK_PARANOIA_FILE_OWN,
                                     rack->paranoia & 
				     PLUGRACK_PARANOIA_FILE_WRITABLE ) ) {
                return 0;
        }

        /*
         * Find the directory name by chopping off the last path element.
         * This also helps weed out malformed file names.  We specify that
         * plugins be specified by fully-qualified pathnames and that means
         * it should have at least one delimiter.
         */
        if ( ( p = strrchr( local, '/' ) ) == NULL ) {
                return 0;
        }
        if ( p != local ) *p = 0;

        return accept_path_paranoia( rack,
                                     local,
                                     rack->paranoia & 
				     PLUGRACK_PARANOIA_DIR_OWN,
                                     rack->paranoia & 
				     PLUGRACK_PARANOIA_DIR_WRITABLE );
}

/*
 * Load a plugin.  Check its type, but not any of the onwership or
 * writability.  It is presumed that those have already been checked.
 */
static plugin_handle_t
plugrack_open_plugin( plugrack_t rack, const char *fq_path )
{
        plugin_handle_t plug;

        if ( ! rack ) return PLUGIN_INVALID_HANDLE;
        if ( ! fq_path ) return PLUGIN_INVALID_HANDLE;
  
        /* See if we can actually load the plugin. */
        plug = plugin_load_from_file( fq_path );
        if ( plug == PLUGIN_INVALID_HANDLE ) return PLUGIN_INVALID_HANDLE;

        /* Now see if this is the right type. */
        if (   rack->major_type 
			   && ( strncmp( rack->major_type,
                          plugin_get_type( plug ),
                          strlen( rack->major_type ) ) != 0 ) ) {
                plugin_unload( plug );
                return PLUGIN_INVALID_HANDLE;
        }

        return plug;
}


plugrack_t plugrack_create( void )
{
        plugrack_t rack = (plugrack_t) xmalloc( sizeof( struct _plugrack ) );

        rack->paranoia     = PLUGRACK_PARANOIA_NONE;
        rack->major_type   = NULL;
        rack->uid          = PLUGRACK_UID_NOBODY;
        rack->entries      = list_create( plugrack_entry_destructor );
        if ( rack->entries == NULL ) {
                xfree( rack );
                return NULL;
        }
        return rack;
}


int
plugrack_destroy( plugrack_t rack )
{
        ListIterator it;
        plugrack_entry_t *e;
  
        if ( ! rack ) return SLURM_ERROR;

        /*
         * See if there are any plugins still being used.  If we unload them,
         * the program might crash because cached virtual mapped addresses
         * will suddenly be outside our virtual address space.
         */
        it = list_iterator_create( rack->entries );
        while ( ( e = list_next( it ) ) != NULL ) {
                if ( e->refcount > 0 ) {
                        list_iterator_destroy( it );
                        return SLURM_ERROR; /* plugins still in use. */
                }
        }
        list_iterator_destroy( it );

        list_destroy( rack->entries );
        xfree( rack );
        return SLURM_SUCCESS;
}


int
plugrack_set_major_type( plugrack_t rack, const char *type )
{
        if ( ! rack ) return SLURM_ERROR;
        if ( ! type ) return SLURM_ERROR;

        /* Free any pre-existing type. */
        if ( rack->major_type ) xfree( rack->major_type );
        rack->major_type = NULL;

        /* Install a new one. */
        if ( type != NULL ) {
                rack->major_type = xstrdup( type );
                if ( rack->major_type == NULL ) return SLURM_ERROR;
        }
  
        return SLURM_SUCCESS;
}


int
plugrack_set_paranoia( plugrack_t rack,
                       const uint32_t flags,
                       const uid_t uid )

{
        if ( ! rack ) return SLURM_ERROR;

        rack->paranoia = flags;
        if ( flags ) {
                rack->uid = uid;
        }

        return SLURM_SUCCESS;
}

static int
plugrack_add_plugin_path( plugrack_t rack,
						  const char *full_type,
						  const char *fq_path )
{
        plugrack_entry_t *e;
  
        if ( ! rack ) return SLURM_ERROR;
        if ( ! fq_path ) return SLURM_ERROR;

        e = (plugrack_entry_t *) xmalloc( sizeof( plugrack_entry_t ) );

        e->full_type = xstrdup( full_type );
        e->fq_path   = xstrdup( fq_path );
        e->plug      = PLUGIN_INVALID_HANDLE;
        e->refcount  = 0;
  
        list_append( rack->entries, e );

        return SLURM_SUCCESS;
}

  
int
plugrack_add_plugin_file( plugrack_t rack, const char *fq_path )
{
		static const size_t type_len = 64;
		char plugin_type[ type_len ];

        if ( ! rack ) return SLURM_ERROR;
        if ( ! fq_path ) return SLURM_ERROR;

        /*
         * See if we should open this plugin.  Paranoia checks must
         * always be done first since code can be executed in the plugin
         * simply by opening it.
         */
        if ( ! accept_paranoia( rack, fq_path ) ) return SLURM_ERROR;

		/* Test the type. */
		if ( plugin_peek( fq_path,
						  plugin_type,
						  type_len,
						  NULL ) == SLURM_ERROR ) {
			return SLURM_ERROR;
		}
		if (   rack->major_type 
			   && ( strncmp( rack->major_type,
							 plugin_type,
                          strlen( rack->major_type ) ) != 0 ) ) {
			return SLURM_ERROR;
        }

        /* Add it to the list. */
        return plugrack_add_plugin_path( rack, plugin_type, fq_path );
}



int
plugrack_read_dir( plugrack_t rack,
                   const char *dir )
{
        char *fq_path;
        char *tail;
        DIR *dirp;
        struct dirent *e;
        struct stat st;
		static const size_t type_len = 64;
		char plugin_type[ type_len ];

        if ( ! rack ) return SLURM_ERROR;
        if ( ! dir ) return SLURM_ERROR;
  
        /* Allocate a buffer for fully-qualified path names. */
        fq_path = alloca( strlen( dir ) + NAME_MAX + 1 );
        xassert( fq_path );

        /*
         * Write the directory name in it, then a separator, then
         * keep track of where we want to write the individual file
         * names.
         */
        strcpy( fq_path, dir );
        tail = &fq_path[ strlen( dir ) ];
        *tail = '/';
        ++tail;

        /* Check whether we should be paranoid about this directory. */
        if ( ! accept_path_paranoia( rack,
                                     dir,
                                     rack->paranoia & 
				     PLUGRACK_PARANOIA_DIR_OWN,
                                     rack->paranoia & 
				     PLUGRACK_PARANOIA_DIR_WRITABLE ) ) {
                return SLURM_ERROR;
        }
  
        /* Open the directory. */
        dirp = opendir( dir );
        if ( dirp == NULL ) return SLURM_ERROR;
  
        while ( 1 ) {
                e = readdir( dirp );
                if ( e == NULL ) break;

                /*
                 * Compose file name.  Where NAME_MAX is defined it represents 
				 * the largest file name given in a dirent.  This macro is used
				 * in the  allocation of "tail" above, so this unbounded copy 
				 * should work.
                 */
                strcpy( tail, e->d_name );

                /* Check only regular files. */
                if ( stat( fq_path, &st ) < 0 ) continue;
                if ( ! S_ISREG( st.st_mode ) ) continue;

                /* See if we should be paranoid about this file. */
                if (!accept_path_paranoia( rack,
                                           dir,
                                           rack->paranoia & 
                                           PLUGRACK_PARANOIA_FILE_OWN,
                                           rack->paranoia & 
                                           PLUGRACK_PARANOIA_FILE_WRITABLE )) {
                        continue;
                }

                /* Test the type. */
				if ( plugin_peek( fq_path,
						   plugin_type,
						   type_len,
						   NULL ) == SLURM_ERROR ) {
					continue;
				}
				if (   rack->major_type 
					   && ( strncmp( rack->major_type,
									 plugin_type,
									 strlen( rack->major_type ) ) != 0 ) ) {
					continue;
				}

                /* Add it to the list. */
                (void) plugrack_add_plugin_path( rack, plugin_type, fq_path );
        }

	closedir( dirp );

        return SLURM_SUCCESS;
}

int
plugrack_read_cache( plugrack_t rack,
                     const char *cache_file )
{
        /* Don't care for now. */
  
        return SLURM_ERROR;
}


int
plugrack_purge_idle( plugrack_t rack )
{
        ListIterator it;
        plugrack_entry_t *e;
  
        if ( ! rack ) return SLURM_ERROR;

        it = list_iterator_create( rack->entries );
        while ( ( e = list_next( it ) ) != NULL ) {
                if ( ( e->plug != PLUGIN_INVALID_HANDLE ) &&
                         ( e->refcount == 0 ) ){
                        plugin_unload( e->plug );
                        e->plug = PLUGIN_INVALID_HANDLE;
                }
        }

        list_iterator_destroy( it );
        return SLURM_SUCCESS;
}


int
plugrack_load_all( plugrack_t rack )
{
        ListIterator it;
        plugrack_entry_t *e;

        if ( ! rack ) return SLURM_ERROR;

        it = list_iterator_create( rack->entries );
        while ( ( e = list_next( it ) ) != NULL ) {
                if ( e->plug == PLUGIN_INVALID_HANDLE ) {
                        (void) plugin_load_from_file( e->fq_path );
                }
        }

        list_iterator_destroy( it );
        return SLURM_SUCCESS;
}


int
plugrack_write_cache( plugrack_t rack,
                      const char *cache )
{
        /* Not implemented. */
  
        return SLURM_SUCCESS;
}

plugin_handle_t
plugrack_use_by_type( plugrack_t rack,
                      const char *full_type )
{
        ListIterator it;
        plugrack_entry_t *e;
  
        if ( ! rack ) return PLUGIN_INVALID_HANDLE;
        if ( ! full_type ) return PLUGIN_INVALID_HANDLE;

        it = list_iterator_create( rack->entries );
        while ( ( e = list_next( it ) ) != NULL ) {
                if ( strcmp( full_type, e->full_type ) != 0 ) continue;

                /* See if plugin is loaded. */
                if ( e->plug == PLUGIN_INVALID_HANDLE ) {
                        e->plug = plugin_load_from_file( e->fq_path );
                }

                /* If load was successful, increment the reference count. */
                if ( e->plug == PLUGIN_INVALID_HANDLE )
                        e->refcount++;

                /*
                 * Return the plugin, even if it failed to load -- this serves
                 * as an error return value.
                 */
                list_iterator_destroy( it );
                return e->plug;
        }

        /* Couldn't find a suitable plugin. */
        list_iterator_destroy( it );
        return PLUGIN_INVALID_HANDLE;
}


int
plugrack_finished_with_plugin( plugrack_t rack, plugin_handle_t plug )
{
        ListIterator it;
        plugrack_entry_t *e;

        if ( ! rack ) return SLURM_ERROR;

        it = list_iterator_create( rack->entries );
        while ( ( e = list_next( it ) ) != NULL ) {
                if ( e->plug == plug ) {
                        e->refcount--;
                        if ( e->refcount < 0 ) e->refcount = 0;

                        /* Do something here with purge policy. */

                        list_iterator_destroy( it );
                        return SLURM_SUCCESS;
                }
        }

        /* Plugin not in this rack. */
        list_iterator_destroy( it );
        return SLURM_ERROR;
}
