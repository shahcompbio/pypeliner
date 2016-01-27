# Road map for future development and planned features

## Support for links

Currently the only way of linking objects is the changeaxis functionality.  A more general mechanism is required.  Each managed object should have the possibility of being a link to another object.  Passing a managed object, modified by .as_link_from() or .as_link_to() would allow passing an identifier to user code.  Links from one resource (file or object) to another could be formed by setting the id given as argument in place of the as_link_from to the argument given in place of an as_link_to.  Refactor of temporary resources would be required.

## Run skip logic

Ability to control which jobs are run and which are skipped is currently very rudimentary.  A user configurable system for picking jobs requiring rerun would be beneficial.  This could be implemented as user provided callbacks, providing the user information such as the job id, the inputs and outputs and their up to date status, as well as long range inputs and their up to date status.

## Temporaries API

To be considered is allowing any managed object to be temporary or user provided.  User provided status would depend on globally setting the filename template or dictionary for the file or object.  This would reduce duplication of code where currently we have to provide the template or filename whenever the managed object is created.

