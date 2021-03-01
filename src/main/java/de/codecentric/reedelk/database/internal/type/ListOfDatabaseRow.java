package de.codecentric.reedelk.database.internal.type;

import de.codecentric.reedelk.runtime.api.annotation.Type;

import java.util.ArrayList;

@Type(listItemType = DatabaseRow.class)
public class ListOfDatabaseRow extends ArrayList<DatabaseRow> {
}
