/**
 * 
 */
{
    function populateHiddenFields(fieldMap) {
        var cursor = db.collection.find();
        cursor.forEach(function(doc) {
            // fieldMap is <index, hiddenField>
            for (var k in fieldMap) {
                // check if there's an array in the index
                var arrayIndex = k.lastIndexOf("*");
                if (arrayIndex > -1) {
                    doArrayMap(doc, k, fieldMap[k], arrayIndex);
                } else {
                    // only update if the matching field exists
                    if(doc[k] !== null) {
                        // if no array, just do a direct update
                	doc[fieldMap[k]] = doc[k].toUpperCase();
                    }
                }
            }
            db.collection.save(doc);
        });
    }

    function doArrayMap(doc, field, hidden, arrayIndex) {
        // get the left and right hand side of the array path so we can loop through
        // pre is the actual array ref
	// post is the field in the array obj
        var fieldPre = field.substring(0, arrayIndex - 1);
        var fieldPost = field.substring(arrayIndex + 2);

        var hiddenPre = hidden.substring(0, arrayIndex - 1);
        var hiddenPost = hidden.substring(arrayIndex + 2);

        // make sure the array exists, if not just break out of this iteration
        if (doc[fieldPre] === null) {
            return;
        }
        for (int i = 0; i < doc[fieldPre].length; i++) {
            // check if there's an array in the index
            var arrayIndex = fieldPost.lastIndexOf("*");
            if (arrayIndex > -1) {
        	// if we have another array, descend
                doArrayMap(doc, fieldPre + i + fieldPost, hiddenPre + i + hiddenPost, arrayIndex)
            } else {
        	// only update if the matching field exists
        	if((doc[fieldPre + i + fieldPost]) !== null) {
        	    // if no more arrays, set the field and continue
        	    doc[hiddenPre + i + hiddenPost] = doc[fieldPre + i + fieldPost].toUpperCase();
        	}
            }
        }
    }
}