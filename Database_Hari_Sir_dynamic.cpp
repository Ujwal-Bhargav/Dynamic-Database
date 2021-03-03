/*
	In this database we need to handle concurrency case of a database

	1)Dynamic allocation i.e., we shld be able to create any number of tables with any number of columns
	2)concurrency
	3)   get--> given id shld return row
		 put-->given  id,vale shld
		 delete--> given id delete row

		 data--array -->row-->array
		 
		 */
#define _CRT_SECURE_NO_WARNINGS
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
struct rows
{
	int current, commit, row_id;
	struct Attributes *columns;
	rows *next;
};

struct Attributes{
	char *column_name;
	char *type;
	char *data;
	int version,current;
	struct Attributes *nextnode;
	struct Attributes *previous;
};
struct response
{
	int version,length;
	struct Attributes *val;
};
struct table{
	struct rows *row;
	int row_count, row_id;
	char* table_name;
	//struct Attributes *column;
};


struct table *new_table;
//struct rows *new_row;
int count, row_count = 1, uid_count = 0, table_index = 0, columns_count = 0, value_count;
struct Attributes* update_address;
struct table* create_table(char* filename)
{
	new_table = (struct table*)malloc(sizeof(struct table)*1);
	new_table->row=NULL;
	new_table->table_name = (char*)malloc(sizeof(char) * 30);
	strcpy(new_table->table_name,filename);
	row_count = 0;
	return new_table;
}
struct rows* get_row(struct table *new_table, int row_id)
{
	struct rows * value = new_table->row;
	if (value==NULL)
		return NULL;
	for (int i = 1; i < row_count; i++)
	{

		if (value->current == row_id)
			return value;
		value = value->next;
	}
	return NULL;
}

struct rows* add_row(struct table *new_table, int row_id)
{
	struct rows *new_row = (struct rows*)malloc(sizeof(struct rows) * 1);
	new_row->commit = 0;
	new_row->current = 1;
	new_row->next = NULL;
	new_row->row_id = row_count++;
	
	new_row->columns = NULL;
	if (new_table->row==NULL)
	{ 
		new_table->row = new_row;
	}
	else
	{
		struct rows* temp=new_table->row;
		while (temp==NULL)
		{
			temp = temp->next;
		}
		temp->next = new_row;
	}
	return new_row;
}

int get_columns_count(struct table *new_table, int row_id, struct rows* row1)
{
	int num = 1;
	struct rows * temp = new_table->row;
	while (temp->row_id != row_id)
	{
		temp = temp->next;
	}
	struct Attributes *temp1 = temp->columns->nextnode;
	while (temp1 != NULL)
	{
		num++;
		temp1 = temp1->nextnode;
	}
	return num;
}
int check_attributes(struct table* new_table, struct rows *row1, int row_id, char* column_name, char*type, char*data)
{
	struct rows *_row = new_table->row;
	if (row1->columns == NULL)
		return NULL;
	else
	{
		
		int flag = 0;
		int columns = get_columns_count(new_table, row_id, row1);
		char *name = (char*)malloc(sizeof(char) * 1024);
		struct Attributes *temp= row1->columns;
		for (int i = 0; i < columns; i++)
		{
			if (!strcmp(temp->column_name, column_name))
			{
				update_address = temp;
				return 1;
			}
				temp = temp->nextnode;
		}
		//update_address = temp;
	}
	return 0;
}

struct Attributes* create_attribute(struct table *new_table, int row_id, char *column_name, char* type, char *data)
{
	struct Attributes* new_cell = (struct Attributes*)malloc(sizeof(struct Attributes) * 1);
	new_cell->column_name = (char*)malloc(sizeof(char) * 1024);
	new_cell->type = (char*)malloc(sizeof(char) * 1024);
	new_cell->data = (char*)malloc(sizeof(char) * 1024);
	strcpy(new_cell->column_name,column_name);
	strcpy(new_cell->type, type);
	strcpy(new_cell->data, data);
	new_cell->version = 1;
	new_cell->current = new_table->row->row_id;
	new_cell->nextnode = NULL;
	new_cell->previous = NULL;
	return new_cell;
}

char **tokenize(char *line){
	char **tokens = (char **)malloc(sizeof(char*) * 10);
	for (int e = 0; e < 10; e++)
		tokens[e] = (char *)malloc(1024);
	int i = 0, c = 0;
	while (*line != ';'){
		if (*line == ' ' || *line == '\n'){
			tokens[c++][i] = '\0';
			i = 0;
		}
		else
			tokens[c][i++] = *line;
		line++;
	}
	tokens[c][i] = '\0';
	return tokens;
}

void update_version_and_data(struct table* new_table, struct rows*row1, int row_id, char *column_name, char*type, char*data)
{
	struct Attributes *_row = (struct Attributes*)malloc(sizeof(struct Attributes));
	int columns = get_columns_count(new_table, row_id, row1);
	struct Attributes *temp = update_address;
	while(temp->previous!=NULL &&!strcmp(temp->column_name, column_name))
		temp = temp->previous;
	temp->previous = _row;
	_row->column_name = (char*)malloc(sizeof(char) * 1024);
	strcpy(_row->column_name, column_name);
	_row->data = (char*)malloc(sizeof(char) * 1024);
	strcpy(_row->data, data);
	_row->nextnode = NULL;
	_row->previous = NULL;
	_row->type = (char*)malloc(sizeof(char) * 1024);
	strcpy(_row->type,type);
	temp->previous->version = temp->version + 1;
	temp->current = temp->previous->version;

/*	_row->columns= (struct Attributes*)malloc(sizeof(struct Attributes) * 1);
	_row->columns->data = (char*)malloc(sizeof(char) * 1024);
	_row->columns->column_name = (char*)malloc(sizeof(char) * 1024);
	_row->columns->type = (char*)malloc(sizeof(char) * 1024);
	_row->commit = row1->commit;
	_row->current = row1->current;
	_row->row_id = row1->row_id;
	strcpy(_row->columns->data,data);
	strcpy(_row->columns->column_name, column_name);
	strcpy(_row->columns->type, type);
	_row->columns->version = row1->columns->version +1;*/

}
void put(struct table *new_table, int row_id, char* column_name, char*type, char* data)
{
	struct rows *row1 = get_row(new_table, row_id);
	if (row1 == NULL)
	{
		struct rows *new_row = add_row(new_table, row_id);
		new_row->columns = create_attribute(new_table, row_id, column_name, type, data);
	}
	else
	{
		int check = check_attributes(new_table, row1, row_id, column_name, type, data);
		if (check == 0)
		{
			struct Attributes* temp = row1->columns;
			while (temp->nextnode!=NULL)
			{
				temp = temp->nextnode;
			}
			temp->nextnode = create_attribute(new_table, row_id, column_name, type, data);
		}
			//row1->columns->nextnode = (struct Attributes*)malloc(sizeof(struct Attributes) * 1);
			 //row1->columns->nextnode = new_column;
		else if (check == NULL)
			row1->columns = create_attribute(new_table, row_id, column_name, type, data);
		else if (check == 1)

			update_version_and_data(new_table, row1, row_id, column_name,type, data);
	
	}

}

/*void insert_into_table()
{
	data_table[count] = (struct table*)malloc(sizeof(struct table)*1);
	data_table[count]->row = (struct rows**)malloc(sizeof(struct rows*) * 1);
	data_table[count]->row[row_count] = (struct rows*)malloc(sizeof(struct rows) * 1);
	data_table[count]->row[row_count]->
}
*/
/*struct rows* insert_into_row(struct Attributes*  cell)
{
	struct Attributes *new_cell = (struct Attributes*)malloc(sizeof(struct Attributes) * 1);
	new_cell->column_name = (char*)malloc(sizeof(char) * 1024);
	new_cell->data = (char*)malloc(sizeof(char) * 1024);
	new_cell->type = (char*)malloc(sizeof(char) * 1024);
	strcpy(new_cell->column_name,cell->column_name);
	strcpy(new_cell->data, cell->data);
	strcpy(new_cell->type, cell->type);
	new_cell->version = 0;
	new_cell->nextnode = NULL;
	new_cell->previous = NULL;
	return new_cell;
}*/
void insert_into_table(struct rows * row)
{
	struct rows * new_row = (struct rows*)malloc(sizeof(struct rows) * 1);
	new_row->columns = row->columns;

}
/*
void insert_into_table(struct rows** row1)
{
	struct rows **new_row = (struct rows**)malloc(sizeof(struct rows*) * 1);
	//row->row[count]->columns = (char*)malloc(sizeof(char) * 1024);
	//new_row->row[count]->commit = row1->commit;
	//new_row->row[count]->current = row1->current;
	//new_row->row[count]->columns=row1->columns;
	new_row[table_index] = (struct rows*)malloc(sizeof(struct rows) * 1);
	new_row[table_index]->//->row = row1;
	//row->columns = row1->columns;
	//row->commit = row1->commit;
	//row->current = row1->commit;
	//row->uid = row1->uid;
	//count += 1;
	//new_table[table_index]->row = row1;
	///new_table[table_index]->row = row1;
	//table_index++;
	//return row;

}
*/
/*
void display()
{
	for (int i = 0; i < count; i++)
	{
		printf("%3d%3d\n", new_table->row[i]->uid,new_table[i]->row[i]->current);
	}
}
*/
struct response** get(struct table* new_table, int row_id)
{
	struct rows* row = get_row(new_table, row_id);
	struct response **value = (struct response**)malloc(sizeof(struct response*) * 100);
	struct Attributes *temp = row->columns;// ->nextnode;
	while (temp != NULL)
	{
		//values[value_count] = (struct response*)malloc(sizeof(struct response) * 1);
		value[value_count] = (struct response*)malloc(sizeof(struct response) * 1);
		value[value_count]->val = (struct Attributes*)malloc(sizeof(struct Attributes) * 1);
		value[value_count]->val->column_name = (char*)malloc(sizeof(char) * 1024);
		value[value_count]->val->type = (char*)malloc(sizeof(char) * 1024);
		value[value_count]->val->data = (char*)malloc(sizeof(char) * 1024);
		strcpy(value[value_count]->val->column_name, temp->column_name);
		value[value_count]->val->current = temp->current;
		strcpy(value[value_count]->val->type, temp->type);
		struct Attributes* temp1 = temp;
		while (temp1->previous != NULL)
		{
			temp1 = temp1->previous;
		}
		value[value_count]->version = temp1->version;
		strcpy(value[value_count]->val->data, temp1->data);
		value_count += 1;
		temp = temp->nextnode;
	}
	value[0]->length = value_count;
	value_count = 0;
	return value;
}
void delete_row(struct table* new_table,int row_id)
{
	struct rows *row = get_row(new_table, row_id);
	free(row);
}
int main()
{
	/*struct table *new_table=create_table();

	put(new_table,1,"Name","string","Ujwal");
	put(new_table, 1, "Name", "string", "ujwal");
	put(new_table, 1, "Name", "string", "Ujjwal");
	put(new_table, 1, "Age", "int", "20");
	put(new_table, 2, "S.No", "int", "1");
	put(new_table, 1, "Age", "int", "21");
	put(new_table, 2, "S.No", "int", "2");
	struct response** ans=get(new_table, 1);
	printf("Column_name\t Type\t Data\t Version\n");
	printf("--------------------------------------\n");
	for (int i = 0; i < ans[0]->length; i++)
	{
		printf("%3s\t\t%3s\t%3s\t%3d\n", ans[i]->val->column_name, ans[i]->val->type, ans[i]->val->data, ans[i]->version);
	}
	system("pause");
	delete_row(new_table,2);*/
	//char *string;
	//struct table *table_name;
	//string = (char*)malloc(sizeof(char)*1024);
	//printf("Enter Query:\n$");
	////scanf("%s",string);
	//gets(string);
	//char **tokens = tokenize(string);
	//printf("%s %s", tokens[1], tokens[2]);
	//if (!strcmp(tokens[0], "create") && (!strcmp(tokens[1],"table")))
	//{
	//	table_name = create_table(tokens[2]);
	//	//printf("YES");
	//}
	//else if (!strcmp(tokens[0], ""))



	struct table *users=create_table();
	put(users, 1, "Name", "string", "Ujwal");
	put(users, 1, "College", "string", "SMEC");
	put(users, 1, "Age", "string", "20");
	struct table *followers = create_table();
	put(followers, 1, "Name", "string", "Bhargav");
	put(followers, 1, "College", "string", "SMEC");
	struct table *postid = create_table();
	put(postid, 1, "Name", "string", "Tadur");
	put(postid, 1, "College", "string", "SMEC");
	
	return 0;
}