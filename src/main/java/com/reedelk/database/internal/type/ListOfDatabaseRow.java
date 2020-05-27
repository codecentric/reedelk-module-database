package com.reedelk.database.internal.type;

import com.reedelk.runtime.api.annotation.Type;

import java.util.ArrayList;

@Type(listItemType = DatabaseRow.class)
public class ListOfDatabaseRow extends ArrayList<DatabaseRow> {
}
