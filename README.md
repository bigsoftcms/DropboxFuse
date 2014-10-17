DropboxFuse
===========

A Fuse filesystem for Dropbox using Python

prerequisites:
* Python 2.7.
* Fuse system library

 		Ubuntu/Debian: sudo apt-get install libfuse
 		Arch: sudo pacman -S fuse
* Python fuse library:

		sudo pip2 install fusepy

* Dropbox python sdk from dropbox dev site:

		https://www.dropbox.com/developers/core/sdks/python
	    
* Dropbox app key and secret: 

		can be obtained by creating an app on the dropbox dev site
		https://www.dropbox.com/developers/apps


Usage:

	mkdir -p /mnt/dropbox
	sudo ./dropbox_fuse.py -m /mnt/dropbox --app-key KEY --app-secret SECRET

A access token is required in order to use the Dropbox API

On first use, the client will walk you through a simple 3-step process.

When the process is complete, the app key, app secret and access token are saved

to the configuration file (located in ${HOME}/.dropboxfuse)


after the first successful login, the app key and secret can be dropped from the cli:

	sudo ./dropbox_fuse.py -m /mnt/dropbox
