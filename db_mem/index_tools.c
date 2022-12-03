#include "index_tools.h"

void init_index_node(idxnode_t *idxnode) {
	bzero(idxnode, sizeof(idxnode_t));
	idxnode->num_children = 0;
	idxnode->is_leaf = true;
	idxnode->parent = (struct idxnode_t *)idxnode;
	idxnode->next = 0;
	idxnode->prev = 0;
}

int num_child_records(idxnode_t *idxnode) {
	int rv = -1;
	if ( !idxnode->is_leaf ) {
		rv = 0;
		for (int i=0; i < idxnode->num_children; i++)
			rv += ((idxnode_t *)((indexkey_t *)(idxnode->children[i]))->childnode)->num_children;
	} else {
		rv = idxnode->num_children;
	}
	return rv;
}

int find_node_index(index_t *idx, idxnode_t *idxnode, char *find_rec, int *index) {
	int rv = 0;
	if ( idxnode->num_children == 0 )
		return rv;

	int i = idxnode->num_children / 2, cmp;

	int lower = 0;
	int upper = idxnode->num_children;

	for ( ;; ) {
		if ( (upper - lower) <= 1 ) {
			while ( i >= 0 && i < idxnode->num_children && (cmp = (*idx->compare_key)(idxnode->children[i], find_rec)) > 0 )
				i--;
			while ( i >= 0 && i < idxnode->num_children && (cmp = (*idx->compare_key)(idxnode->children[i], find_rec)) < 0 )
				i++;
			break;
		} else if ( (*idx->compare_key)(idxnode->children[i], find_rec) < 0 ) {
			lower = i;
		} else {
			upper = i;
		}

		i = lower + ((upper - lower) / 2);
	}

	if ( i < 0 ) {
		rv = -1;
		*index = 0;
	} else if ( i >= idxnode->num_children ) {
		rv = 1;
		*index  = idxnode->num_children - 1;
	} else {
		rv = 0;
		*index = i;
	}

	return rv;
}

idxnode_t *find_node(index_t *idx, idxnode_t *idxnode, char *find_rec) {
	if ( idxnode->is_leaf )
			return idxnode;

	int index = 0, found;
	idxnode_t *current = idxnode;
	while ( (found = find_node_index(idx, current, find_rec, &index)) >= 1)
		if ( (idxnode_t *)current->next == NULL )
			break;
		else
			current = (idxnode_t *)current->next;

	return find_node(idx, ((indexkey_t *)(current->children[index]))->childnode, find_rec);
}

char *find_record(index_t *idx, idxnode_t *idxnode, char *find_rec) {
	char *rv = NULL;

	idxnode = find_node(idx, idxnode, find_rec);
	int index = 0, found = 0;

	// edge case, empty index
	if ( idxnode->num_children == 0 )
		return rv;

	idxnode_t *current = idxnode;
	while ( (found = find_node_index(idx, current, find_rec, &index)) == 1)
		if ( (idxnode_t *)current->next == NULL )
			break;
		else
			current = (idxnode_t *)current->next;

	if ( found == 0 && (*idx->compare_key)(current->children[index], find_rec) == 0 )
		rv = (char *)current->children[index];

	return rv;
}

idxnode_t *split_node(index_t *idx, idxnode_t *idxnode, char *key) {
	uint16_t nc = idxnode->num_children / 2;
	idxnode_t *rv = 0;

	// determine if the new value causing the split will be on the right or left side of the split
	// and make that node slightly emptier
	if ( (*idx->compare_key)(idxnode->children[nc], key) < 0 )
		nc++;

	if ( (char *)idxnode->parent != (char *)idxnode ) {
		// check lower range for availability, if we can locate there
		if ( idxnode->prev != NULL && idxnode->is_leaf && (*idx->compare_key)(idxnode->children[0], key) > 0 )
			if ( ((idxnode_t *)idxnode->prev)->num_children < IDX_ORDER )
				return (idxnode_t *)idxnode->prev;

		// split node if there is no room
		if ( ((idxnode_t *)idxnode->parent)->num_children >= IDX_ORDER )
			split_node(idx, (idxnode_t *)idxnode->parent, key);

		idxnode_t *new_node, *child_node;
		new_node = malloc(sizeof(idxnode_t));
		bzero(new_node, sizeof(idxnode_t));

		void *new_k = (*idx->create_key)();

		new_node->is_leaf = idxnode->is_leaf;
		new_node->parent = idxnode->parent;
		new_node->prev = idxnode->prev;
		if ( new_node->prev != NULL )
			((idxnode_t *)new_node->prev)->next = (struct idxnode_t *)new_node;

		new_node->next = (struct idxnode_t *)idxnode;
		idxnode->prev = (struct idxnode_t *)new_node;

		for(int i=0; i<nc; i++) {
			if( !new_node->is_leaf ) {
				child_node = (idxnode_t *)((indexkey_t *)idxnode->children[i])->childnode;
				child_node->parent = (struct idxnode_t *)new_node;
			}
			new_node->children[i] = idxnode->children[i];
			new_node->num_children++;
		}

		(*idx->copy_key)(new_node->children[new_node->num_children-1], new_k);
		((indexkey_t *)new_k)->childnode = new_node;

		add_node_value(idx, (idxnode_t *)idxnode->parent, new_k);

		for(int i=0; i<idxnode->num_children - nc; i++) {
			idxnode->children[i] = idxnode->children[nc+i];
			idxnode->children[nc+i] = 0;
		}

		idxnode->num_children -= nc;

		if ( (*idx->compare_key)(new_node->children[new_node->num_children - 1], key) < 0 &&
				(*idx->compare_key)(idxnode->children[0], key) > 0 ) {
			if ( new_node->num_children >= idxnode->num_children ) {
				rv = idxnode;
			} else {
				rv = new_node;
			}

		} else if ( (*idx->compare_key)(new_node->children[new_node->num_children - 1], key) > 0 ) {
			rv = new_node;

		} else {
			rv = idxnode;

		}

	} else if ( (char *)idxnode->parent == (char *)idxnode ) {
		idxnode_t *new_left, *new_right, *child_node;
		void *new_left_k, *new_right_k;
		void *old_left_k = 0, *old_right_k = 0;

		new_left = malloc(sizeof(idxnode_t));
		new_right = malloc(sizeof(idxnode_t));
		bzero(new_left, sizeof(idxnode_t));
		bzero(new_right, sizeof(idxnode_t));

		new_left_k = (*idx->create_key)();
		new_right_k = (*idx->create_key)();

		new_left->is_leaf = idxnode->is_leaf;
		new_left->parent = (struct idxnode_t *)idxnode;
		for(int i=0; i < nc; i++) {
			if( !new_left->is_leaf ) {
				child_node = ((indexkey_t *)(idxnode->children[i]))->childnode;
				child_node->parent = (struct idxnode_t *)new_left;
			}
			new_left->children[i] = idxnode->children[i];
			old_left_k = new_left->children[i];
			idxnode->children[i] = 0;
			new_left->num_children++;
		}

		(*idx->copy_key)(old_left_k, new_left_k);
		((indexkey_t *)new_left_k)->childnode = new_left;

		new_right->is_leaf = idxnode->is_leaf;
		new_right->parent = (struct idxnode_t *)idxnode;
		for(int i=nc; i < idxnode->num_children; i++) {
			if( !new_right->is_leaf ) {
				child_node = ((indexkey_t *)(idxnode->children[i]))->childnode;
				child_node->parent = (struct idxnode_t *)new_right;
			}
			new_right->children[i - nc] = idxnode->children[i];
			old_right_k = new_right->children[i - nc];
			idxnode->children[i] = 0;
			new_right->num_children++;
		}

		(*idx->copy_key)(old_right_k, new_right_k);
		((indexkey_t *)new_right_k)->childnode = new_right;

		idxnode->is_leaf = false;
		idxnode->num_children = 2;
		idxnode->children[0] = new_left_k;
		idxnode->children[1] = new_right_k;

		new_left->next = (struct idxnode_t *)new_right;
		new_right->prev = (struct idxnode_t *)new_left;

		if ( (*idx->compare_key)(new_left->children[new_left->num_children - 1], key) < 0 &&
				(*idx->compare_key)(new_right->children[0], key) > 0 ) {
			if ( new_left->num_children >= new_right->num_children ) {
				rv = new_right;
			} else {
				rv = new_left;
			}
		} else if ( (*idx->compare_key)(new_left->children[new_left->num_children - 1], key) > 0 ) {
			rv = new_left;
		} else {
			rv = new_right;
		}
	}

	return rv;
}

void update_max_value (index_t *idx, idxnode_t *parent_idx, idxnode_t *idxnode, char *new_key) {
	indexkey_t *current_key;
	int i = 0;

	if ( (char *)parent_idx != (char *)idxnode ) {
		for(i=0; i < parent_idx->num_children; i++) {
			current_key = (indexkey_t *)parent_idx->children[i];
			if ( ((indexkey_t *)current_key)->childnode == idxnode ) {
				(*idx->copy_key)(new_key, (char *)current_key);
				((indexkey_t *)current_key)->childnode = idxnode;
				break;
			}
		}
		if ( i == parent_idx->num_children - 1)
			update_max_value(idx, (idxnode_t *)parent_idx->parent, (idxnode_t *)parent_idx, new_key);
	}
}

idxnode_t *add_node_value (index_t *idx,idxnode_t *idxnode, char *key) {

	if ( idxnode->num_children >= IDX_ORDER )
		idxnode = split_node(idx, idxnode, key);

 	int i = 0;
	for( i=0; i < idxnode->num_children && (*idx->compare_key)(idxnode->children[i], key) < 0; i++ );

	if(i < idxnode->num_children) {
		memmove(idxnode->children + i + 1, idxnode->children + i, sizeof(char *) * (idxnode->num_children - i));
	} else if ( i == idxnode->num_children ) {
		update_max_value(idx, (idxnode_t *)idxnode->parent, (idxnode_t *)idxnode, key);
	}

	(idxnode->num_children)++;

	/*
		new keys for leaf nodes are externally allocated and must be copied
		node keys are internally allocated and can be pointed to immeediately and will
		be released properly when cleaned up
	*/
	if ( !idxnode->is_leaf ) {
		idxnode->children[i] = key;
	} else {
		indexkey_t *new_key = (indexkey_t *)(*idx->create_key)();
		(*idx->copy_key)(key, (char *)new_key);
		idxnode->children[i] = (char *)new_key;
	}

	return idxnode;
}

bool add_index_value (index_t *idx, idxnode_t *idxnode, char *key) {
	bool rv = false;
	if ( idxnode->is_leaf ) {
		if ( idx->is_unique ) {
			bool found = true;
			found = find_record(idx, idxnode, key) != NULL;
			if ( found )
				return rv;
		}
		add_node_value(idx, idxnode, key);
		rv = true;
	} else {
		int index = 0, i;

		/*
		check the middle-ish node to see if our id is higher or lower than that and
		determine if it'll be i++ or i--
		*/

		if ( idxnode->num_children > 0 ) {
			i = idxnode->num_children / 2;
			index = i;

			if ( (*idx->compare_key)(idxnode->children[i], key) < 0 ) {
				for ( ; i < idxnode->num_children; i++ ) {
					index = i;
					if ( (*idx->compare_key)(idxnode->children[i], key) > 0 )
						break;
				}
			} else {
				for ( ; i >= 0; --i ) {
					if ( (*idx->compare_key)(idxnode->children[i], key) < 0 )
						break;

					index = i;
				}
			}
		}

		add_index_value(idx, ((indexkey_t *)(idxnode->children[index]))->childnode, key);
	}
	return rv;
}

void collapse_nodes(idxnode_t *idxnode) {
	if ( idxnode->is_leaf )
			return;

	int nc = num_child_records(idxnode);

	//should this be < or <=?
	if ( nc <= IDX_ORDER && nc > 0 ) {

		idxnode_t *cn = ((indexkey_t *)(idxnode->children[0]))->childnode;
		void *children[IDX_ORDER];
		int index = 0;

		idxnode->is_leaf = cn->is_leaf;

		for(int i=0; i < idxnode->num_children; i++) {
			cn = ((indexkey_t *)(idxnode->children[i]))->childnode;
			for (int k=0; k < cn->num_children; k++) {
				children[index] = cn->children[k];
				if ( !idxnode->is_leaf )
					((idxnode_t *)((indexkey_t *)children[index])->childnode)->parent = (struct idxnode_t *)idxnode;
				index++;
			}
			free(cn);
		}

		for(int i=0; i < IDX_ORDER; i++) {
			if ( i < idxnode->num_children )
				free(idxnode->children[i]);
			idxnode->children[i] = 0;
		}
		idxnode->num_children = 0;

		for(int i=0; i < index; i++) {
			(idxnode->num_children)++;
			idxnode->children[i] = children[i];
		}
	}
}

bool remove_node_value(index_t *idx, idxnode_t *idxnode, char *key) {
	void *v;
	//char msg[128];
	bool success = false;
	int merge_amt = IDX_ORDER / 2;

	while ( (*idx->compare_key)(idxnode->children[0], key) <= 0 ) {
		for ( int i = 0; i < idxnode->num_children; i++ ) {
			if ( (*idx->compare_key)(idxnode->children[i], key) == 0 ) {
				v = idxnode->children[i];

				for ( int k=i+1; k < idxnode->num_children; k++)
					idxnode->children[k-1] = idxnode->children[k];

				(idxnode->num_children)--;
				idxnode->children[idxnode->num_children] = 0;
				free(v);

				if ( idxnode->num_children > 0 && idxnode->num_children <= merge_amt ) {
					int free_count = 0;

					if ( idxnode->prev != NULL )
						free_count += IDX_ORDER - ((idxnode_t *)idxnode->prev)->num_children;
					if ( idxnode->next != NULL )
						free_count += IDX_ORDER - ((idxnode_t *)idxnode->next)->num_children;

					if ( free_count > idxnode->num_children ) {
						int move_left, move_right, free_left, free_right;
						idxnode_t *c;

						free_left = idxnode->prev == NULL ? 0 : IDX_ORDER - ((idxnode_t *)idxnode->prev)->num_children;
						free_right = idxnode->next == NULL ? 0 : IDX_ORDER - ((idxnode_t *)idxnode->next)->num_children;

						if ( free_left > free_right ) {
							move_right =  idxnode->num_children / 2;
							move_left = idxnode->num_children - move_right;

						} else {
							move_left =  idxnode->num_children / 2;
							move_right = idxnode->num_children - move_left;

						}

						while ( free_left < move_left || free_right < move_right ) {
							if ( free_left < move_left ) {
								move_left--;
								move_right++;
							} else {
								move_left++;
								move_right--;
							}
						}

						if ( move_right > 0 && idxnode->next != NULL ) {
							c = (idxnode_t *)idxnode->next;
							memmove(c->children + move_right, c->children, sizeof(char *) * c->num_children);
							for ( int k = 0; k < move_right; k++ ) {
								c->children[k] = idxnode->children[move_left + k];
								if ( !c->is_leaf )
									((idxnode_t *)(((indexkey_t *)(c->children[k])))->childnode)->parent = (struct idxnode_t *)c;

								(c->num_children)++;
								idxnode->children[move_left + k] = 0;
							}
						}

						if ( move_left > 0 && idxnode->prev != NULL ) {
							c = (idxnode_t *)idxnode->prev;
							for ( int k = 0; k < move_left; k++ ) {
								c->children[c->num_children] = idxnode->children[k];
								if ( !c->is_leaf )
									((idxnode_t *)((indexkey_t *)(c->children[c->num_children]))->childnode)->parent = (struct idxnode_t *)c;

								(c->num_children)++;
								idxnode->children[k] = 0;
							}
							update_max_value(idx, (idxnode_t *)c->parent, (idxnode_t *)c, c->children[c->num_children-1]);
						}

						(idxnode->num_children) -= move_right;
						(idxnode->num_children) -= move_left;
						/*
						Figure how much may be distrubted left (if any) and right (if any)
						Attempt to weight the distribution to the 'more empty' side
						*/
						//memmove(idxnode->children + i + 1, idxnode->children + i, sizeof(void *) * (idxnode->num_children - i));
					}
				}

				if ( (char *)idxnode != (char *)idxnode->parent ) {
					if ( idxnode->num_children == 0 ) {
						if ( idxnode-> prev != NULL )
							((idxnode_t *)idxnode->prev)->next = idxnode->next;

						if ( idxnode->next != NULL )
							((idxnode_t *)idxnode->next)->prev = idxnode->prev;

						for ( int k=0; k < ((idxnode_t *)idxnode->parent)->num_children; k++) {
							if ( ((indexkey_t *)(((idxnode_t *)idxnode->parent)->children[k]))->childnode == idxnode ) {
								remove_node_value(idx, (idxnode_t *)idxnode->parent, ((idxnode_t *)idxnode->parent)->children[k]);
								break;
							}
						}
						free(idxnode);
					} else {
						if (i == idxnode->num_children ) {
							update_max_value(idx, (idxnode_t *)idxnode->parent, (idxnode_t *)idxnode, idxnode->children[i-1]);
						}
					}
				}

				success = true;
				break;
			}
		}

		if ( success ) {
			break;

		} else if ( idxnode->next == NULL ) {
			break;

		} else {
			idxnode = (idxnode_t *)idxnode->next;

		}
	}

	return success;
}

void read_index_from_file(index_t *idx) {
	char *ipth;
	if ( (ipth = getenv("TABLE_DATA")) == NULL )
		ipth = DEFAULT_BASE;

	int fd = -1;
	size_t sz = strlen(ipth) + 1 + strlen(idx->index_name) + 5;

	// path + '/' + name + '.idx' + \0
	char *idxfile = malloc(sz);
	bzero(idxfile, sz);

	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idx");

	printf("Reading keys from file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		fprintf(stderr, "Error: %s\n", strerror(errno));
	}
	if ( (fd = open(idxfile, O_RDONLY, 0640)) >= 0 ) {
		uint16_t recordsize = 0;
		uint64_t recordcount = 0;
		uint64_t counter = 0;

		if ( read(fd, &recordsize, sizeof(uint16_t)) != sizeof(uint16_t) ) {
			printf("Incomplete read of recordsize\n");
		}
		if ( read(fd, &recordcount, sizeof(uint64_t)) != sizeof(uint64_t) ) {
			printf("Incomplete read of recordcount\n");
		}
		printf("%" PRIu64 " records of size %"PRIu16" exist in index file\n", recordcount, recordsize);
		void *buff = malloc(sizeof(char)*recordsize);
		for(counter = 0; counter < recordcount; counter++) {
			bzero(buff, sizeof(char)*recordsize);
			if ( read(fd, buff, recordsize) != recordsize ) {
				// an index needs to be remapped in some way
			} else {
				add_index_value(idx, &idx->root_node, buff);
			}
		}
		free(buff);
	} else
		fprintf(stderr, "Error: %s\n", strerror(errno));

	free(idxfile);
}

void write_file_from_index(index_t *idx) {
	int fd = -1;
	char *ipth;
	uint16_t recordsize = idx->record_size;

	void *buffer = malloc(sizeof(char)*recordsize);

	if ( (ipth = getenv("TABLE_DATA")) == NULL )
		ipth = DEFAULT_BASE;

	size_t sz = strlen(ipth) + 1 + strlen(idx->index_name) + 5;

	// path + '/' + name + '.idx' + \0
	char *idxfile = malloc(sz);
	bzero(idxfile, sz);

	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idx");

	printf("Writing keys to file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		fprintf(stderr, "Error: %s\n", strerror(errno));
	}

	if ( (fd = open(idxfile, O_CREAT | O_RDWR | O_TRUNC, 0640)) >= 0 ) {
		idxnode_t *cn = &idx->root_node;

		while(!cn->is_leaf)
			cn = ((indexkey_t *)(cn->children[0]))->childnode;

		uint64_t recordcount = 0;

		write(fd, &recordsize, sizeof(recordsize));
		write(fd, &recordcount, sizeof(recordcount));
		while ( cn != NULL ) {
			for(i = 0; i < cn->num_children; i++) {
				bzero(buffer, sizeof(char)*recordsize);
				memcpy(buffer, cn->children[i], sizeof(char)*recordsize);
				write(fd, buffer, sizeof(char)*recordsize);
				recordcount++;
			}
			cn = (idxnode_t *)cn->next;
		}
		lseek(fd, sizeof(recordsize), SEEK_SET);
		write(fd, &recordcount, sizeof(recordcount));
		close(fd);

		printf("%" PRIu64 " records of size %"PRIu16" written\n", recordcount, recordsize);
	}
	free(idxfile);
	free(buffer);
}

bool remove_index_value (index_t *idx, idxnode_t *idxnode, char *key) {
	idxnode_t *leaf_node = find_node(idx, idxnode, key);

	bool success = remove_node_value(idx, leaf_node, key);

	if ( (char *)idxnode == (char *)idxnode->parent )
			collapse_nodes(idxnode);

	return success;
}

void release_tree(index_t *idx, idxnode_t *idxnode) {
	if ( !idxnode->is_leaf ) {
		idxnode->is_leaf = true;
		for(int i=0; i < idxnode->num_children; i++) {
			idxnode_t *c = ((indexkey_t *)(idxnode->children[i]))->childnode;
			release_tree(idx, c);
			free(c);
			free(idxnode->children[i]);
		}
		idxnode->num_children = 0;
	} else {
		for(int i=0; i < idxnode->num_children; i++)
			free(idxnode->children[i]);
		idxnode->num_children = 0;
	}
}

void print_tree(index_t *idx, idxnode_t *idxnode, int *counter) {
	idxnode_t *s = idxnode;
	int page = 1;

	printf("Level %d:", *counter);
	do {
		for(int i=0; i < idxnode->num_children; i++) {
			// This is hardcoded and shouldn't be
			char strkey[64];
			(*idx->print_key)(idxnode->children[i], strkey);
			printf(" %s (%s %d)", strkey, idxnode->is_leaf ? "leaf" : "node", page);
		}
		idxnode = (idxnode_t *)idxnode->next;
		page++;
	} while ( idxnode != NULL );
	printf("\n");

	(*counter)++;
	if ( !s->is_leaf )
		print_tree(idx, ((indexkey_t *)(s->children[0]))->childnode, counter);
}

void print_tree_totals(index_t *idx, idxnode_t *idxnode, int *counter) {
	idxnode_t *s = idxnode;
	uint64_t children = 0;

	do {
		children += idxnode->num_children;
		idxnode = (idxnode_t *)idxnode->next;
	} while ( idxnode != NULL );
	printf("Level %d: %" PRIu64 " children\n", *counter, children);

	(*counter)++;
	if ( !s->is_leaf )
		print_tree_totals(idx, ((indexkey_t *)(s->children[0]))->childnode, counter);
}

void print_index_scan_lookup(index_t *idx, char *key) {
	printf("Performing an index scan\n");
	idxnode_t *i = &idx->root_node;
	while ( !i->is_leaf )
		i = ((indexkey_t *)i->children[0])->childnode;

	char left[128], right[128], msg[128];
	(*idx->print_key)(i->children[0], left);
	(*idx->print_key)(i->children[i->num_children-1], right);

	printf("Starting with %p (%s -> %s)\n",
			(void *)i,
			left,
			right);
	int index = -1;
	uint64_t leafcounter = 0;

	do {
		leafcounter++;
		for ( index = 0; index < i->num_children; index++ )
			if ( (*idx->compare_key)(i->children[index], key) >= 0 )
				break;

		if ( index < i->num_children )
			break;
		else
			i = (idxnode_t *)i->next;
	} while ( i != NULL );

	if ( i !=  NULL &&
			index < i->num_children &&
			(*idx->compare_key)(i->children[index], key) == 0) {

		(*idx->print_key)(i->children[index], msg);
		printf("Found key %s on %p in index %d %" PRIu64 " leaves in\n",
				msg,
				(void *)i,
				index,
				leafcounter);
		for( ;; ) {
			(*idx->print_key)(i->children[0], left);
			(*idx->print_key)(i->children[i->num_children-1], right);

			printf("%p %s (%s -> %s)\n",
					(void *)i,
					(void *)i == (void *)i->parent ? "root" : i->is_leaf ? "leaf" : "node",
					left,
					right
					);
			if ( i == (idxnode_t *)i->parent )
				break;
			i = (idxnode_t *)i->parent;
		}
	} else {
		(*idx->print_key)(key, msg);
		printf("Key %s could not be found in %" PRIu64 " leaves\n", msg, leafcounter);
	}
}

/* new data dictionary functions */

db_idxnode_t * dbidx_init_root_node(db_index_schema_t *idx) {
	db_idxnode_t *idxnode = dbidx_allocate_node(idx);
	idxnode->is_leaf = true;
	idxnode->parent = idxnode;
	return idxnode;
}

db_idxnode_t *dbidx_allocate_node(db_index_schema_t *idx) {
	db_idxnode_t *rv = NULL;

	if ( idx == NULL )
		return rv;

	size_t nodesz = sizeof(db_idxnode_t) + sizeof(db_indexkey_t *) * idx->index_order;
	rv = malloc(nodesz);
	bzero(rv, nodesz);
	rv->nodesz = nodesz;
	rv->parent = NULL;
	rv->next = NULL;
	rv->prev = NULL;
	rv->children = (db_indexkey_t **)((char *)rv + sizeof(db_idxnode_t));
	return rv;
}

db_indexkey_t *dbidx_allocate_key(db_index_schema_t *idx) {
	db_indexkey_t *rv = NULL;

	if ( idx == NULL )
		return rv;

	/* This is kind of an unsafe strategy, but it should work */
	size_t keysz = sizeof(db_indexkey_t) + sizeof(char *) * idx->num_fields;
	rv = malloc(keysz);
	bzero(rv, keysz);
	rv->childnode = NULL;
	rv->keysz = keysz;
	rv->data = (char **)((char *)rv + sizeof(db_indexkey_t));
	return rv;
}

void dbidx_reset_key(db_index_schema_t *idx, db_indexkey_t *key) {
	size_t keysz = sizeof(db_indexkey_t) + sizeof(char *) * idx->num_fields;
	bzero(key, keysz);
	key->childnode = NULL;
	key->keysz = keysz;
	key->data = (char **)((char *)key + sizeof(db_indexkey_t));
}

void dbidx_reset_key_with_data(db_index_schema_t *idx, db_indexkey_t *key) {
	size_t keysz = sizeof(db_indexkey_t) + sizeof(char *) * idx->num_fields + sizeof(char *) * idx->record_size;
	bzero(key, keysz);
	key->keysz = keysz;
	key->data = (char **)((char *)key + sizeof(db_indexkey_t));
	*key->data = (char *)key + sizeof(db_indexkey_t) + sizeof(char *) * idx->num_fields;
}

char *dbidx_allocate_key_data(db_index_schema_t *idx) {
	char *rv = NULL;

	if ( idx == NULL )
		return rv;
	rv = malloc(idx->record_size);
	bzero(rv, idx->record_size);
	return rv;
}

db_indexkey_t *dbidx_allocate_key_with_data(db_index_schema_t *idx) {
	db_indexkey_t *rv = NULL;

	if ( idx == NULL )
		return rv;

	/* This is kind of an unsafe strategy, but it should work */
	size_t keysz = sizeof(db_indexkey_t) + sizeof(char *) * idx->num_fields + sizeof(char *) * idx->record_size;
	//printf("Allocating a total of %ld bytes (%ld %ld %ld)\n", keysz, sizeof(db_indexkey_t), sizeof(char *) * idx->fields_sz, sizeof(char *) * idx->record_size);
	rv = malloc(keysz);
	bzero(rv, keysz);
	rv->keysz = keysz;
	rv->data = (char **)((char *)rv + sizeof(db_indexkey_t));
	*rv->data = (char *)rv + sizeof(db_indexkey_t) + sizeof(char *) * idx->num_fields;
	return rv;
}

bool dbidx_copy_key(db_indexkey_t *src, db_indexkey_t *dst) {
	memcpy(dst, src, src->keysz);
	dst->childnode = NULL;
	dst->data = (char **)((char *)dst + sizeof(db_indexkey_t));
	return true;
}

void dbidx_release_tree(db_index_t *idx, db_idxnode_t *idxnode) {
	db_idxnode_t *current_node = idxnode;
	if ( current_node == NULL )
		current_node = idx->root_node;

	if ( current_node == NULL )
		return;

	if ( !current_node->is_leaf ) {
		for(index_order_t i = 0; i < current_node->num_children; i++) {
			dbidx_release_tree(idx, current_node->children[i]->childnode);
			free(current_node->children[i]);
			current_node->children[i] = NULL;
		}
	} else {
		for(index_order_t i = 0; i < current_node->num_children; i++)
			if ( current_node->children[i] != NULL ) {
				free(current_node->children[i]);
				current_node->children[i] = NULL;
			}
	}
	free(current_node);
	current_node = NULL;
}

bool dbidx_set_key_data_field_value(db_index_schema_t *idx, char *field_name, char *data, char *value) {
	size_t offset = 0;
	dd_datafield_t *f = NULL;
	for(uint8_t i = 0; i < idx->num_fields; i++) {
		f = idx->fields[i];
		if ( strcmp(f->field_name, field_name) == 0 ) {
			memcpy(data + offset, value, f->field_sz);
			return true;
		} else {
			offset += f->field_sz;
		}
	}
	return false;
}
bool dbidx_set_key_field_value(db_index_schema_t *idx, char *field_name, db_indexkey_t *key, char *value) {
	dd_datafield_t *f = NULL;
	for(uint8_t i = 0; i < idx->num_fields; i++) {
		f = idx->fields[i];
		if ( strcmp(f->field_name, field_name) == 0 ) {
			bzero(key->data[i], f->field_sz);
			size_t fieldsz = f->field_sz;
			if ( f->fieldtype == STR )
				fieldsz = strlen(value);
			memcpy(key->data[i], value, fieldsz);
			return true;
		}
	}
	return false;
}

signed char dbidx_compare_keys(db_index_schema_t *idx, db_indexkey_t *keya, db_indexkey_t *keyb) {
	signed char rv = 0;
	bool a, b;

	if ( idx == NULL || keya == NULL || keyb == NULL )
		return -2;

	for(uint8_t i = 0; i < idx->num_fields; i++) {
		switch (idx->fields[i]->fieldtype) {
		case STR:
			rv = str_compare_sz(keya->data[i], keyb->data[i], idx->fields[i]->field_sz);
			break;
		case TIMESTAMP:
			rv = ts_compare((struct timespec *)keya->data[i], (struct timespec *)keyb->data[i]);
			break;
		case BOOL:
			a = *(bool *)keya->data[i];
			b = *(bool *)keyb->data[i];
			rv = a == b ? 0 : a > b ? 1 : -1;
			break;
		case UUID:
			rv = uuid_compare(*((uuid_t *)keya->data[i]), *((uuid_t *)keyb->data[i]));
			break;
		case UI64:
			rv = ui64_compare((uint64_t *)keya->data[i], (uint64_t *)keyb->data[i]);
			break;
		case I64:
			rv = i64_compare((int64_t *)keya->data[i], (int64_t *)keyb->data[i]);
			break;
		case UI32:
			rv = ui32_compare((uint32_t *)keya->data[i], (uint32_t *)keyb->data[i]);
			break;
		case I32:
			rv = i32_compare((int32_t *)keya->data[i], (int32_t *)keyb->data[i]);
			break;
		case UI16:
			rv = ui16_compare((uint16_t *)keya->data[i], (uint16_t *)keyb->data[i]);
			break;
		case I16:
			rv = i16_compare((int16_t *)keya->data[i], (int16_t *)keyb->data[i]);
			break;
		case UI8:
			rv = ui8_compare((uint8_t *)keya->data[i], (uint8_t *)keyb->data[i]);
			break;
		case I8:
			rv = i8_compare((int8_t *)keya->data[i], (int8_t *)keyb->data[i]);
			break;
		case BYTES:
			rv = bytes_compare((unsigned char *)keya->data[i], (unsigned char *)keyb->data[i], idx->fields[i]->field_sz);
			break;
		default:
			rv = -2;
		}
		if ( rv != 0 )
			break;
	}

	if ( rv == 0 && keya->record < UINT64_MAX && keyb->record < UINT64_MAX )
		rv = keya->record == keyb->record ? 0 : keya->record > keyb->record ? 1 : -1;

	return rv;
}

uint64_t dbidx_num_child_records(db_idxnode_t *idxnode) {
	if ( idxnode->is_leaf )
		return (uint64_t)idxnode->num_children;

	int rv = 0;
	for (int i=0; i < idxnode->num_children; i++)
		rv += (uint64_t)((db_idxnode_t *)idxnode->children[i])->num_children;

	return rv;
}

db_indexkey_t *dbidx_find_record(db_index_schema_t *idx, db_idxnode_t *idxnode, db_indexkey_t *find_rec) {
	db_indexkey_t *rv = NULL;

	idxnode = dbidx_find_node(idx, idxnode, find_rec);
	index_order_t index = 0;
	signed char found = 0;

	// edge case, empty index
	if ( idxnode->num_children == 0 )
		return rv;

	db_idxnode_t *current = idxnode;
	while ( (found = dbidx_find_node_index(idx, current, find_rec, &index)) == 1)
		if ( current->next == NULL )
			break;
		else
			current = current->next;

	if ( found == 0 && dbidx_compare_keys(idx, current->children[index], find_rec) == 0 )
		rv = current->children[index];

	return rv;

}

signed char dbidx_find_node_index(db_index_schema_t *idx, db_idxnode_t *idxnode, db_indexkey_t *find_rec, index_order_t *index) {
	signed char rv = 0;
	if ( idxnode->num_children == 0 )
		return rv;

	int64_t i = idxnode->num_children / 2;

	index_order_t lower = 0;
	index_order_t upper = idxnode->num_children;

	for ( ;; ) {
		/*
		 * this code continually takes the list of keys and divides it in half to check if the candidate key is greater or
		 * smaller than the midpoint, for example:
		 * find rec == 8
		 * array is [3, 4, 6, 7, 9, 11]
		 * lower = position 0 (value 3)
		 * upper = position 5 (value 11)
		 * 5 - 0 / 2 = 2 (position 2 is a value of 6)
		 * 8 > 6 so the lower bound is moved from 0 -> 2
		 * 2 + ((5 - 2) / 2) = 2 + 1 = position 3 value of 7 which is less than 8
		 * 3 + ((5 - 3) / 2) = 2 + 2 = position 4 value of 9 which is greater than 8
		 *   - the the upper bound is now set to 4
		 * 3 + ((4 - 3) / 2)) = 3 + 0 = position 3 and 4 - 3 is now <= 1
		 *   - we enter the two loops to be just less than position 4 (value of 9)
		 */

		if ( (upper - lower) <= 1 ) {
			while ( i >= 0 && i < idxnode->num_children && dbidx_compare_keys(idx, idxnode->children[i], find_rec) > 0 )
				i--;
			while ( i >= 0 && i < idxnode->num_children && dbidx_compare_keys(idx, idxnode->children[i], find_rec) < 0 )
				i++;
			break;
		} else if ( dbidx_compare_keys(idx, idxnode->children[i], find_rec) < 0 ) {
			lower = i;
		} else {
			upper = i;
		}

		i = lower + ((upper - lower) / 2);
	}

	if ( i < 0 ) {
		rv = -1;
		*index = 0;
	} else if ( i >= idxnode->num_children ) {
		rv = 1;
		*index  = idxnode->num_children - 1;
	} else {
		rv = 0;
		*index = i;
	}

	return rv;
}

db_idxnode_t *dbidx_find_node(db_index_schema_t *idx, db_idxnode_t *idxnode, db_indexkey_t *find_rec) {
	if ( idxnode->is_leaf )
			return idxnode;

	index_order_t index = 0;
	signed char found = 0;
	db_idxnode_t *current = idxnode;

	while ( (found = dbidx_find_node_index(idx, current, find_rec, &index)) > 1)
		if ( current->next == NULL )
			break;
		else
			current = current->next;

	return dbidx_find_node(idx, current->children[index]->childnode, find_rec);
}

bool dbidx_add_index_value (db_index_t *idx, db_idxnode_t *idxnode, db_indexkey_t *key) {
	bool rv = false;
	db_idxnode_t *current = idxnode;
	if ( current == NULL )
		current = idx->root_node;

	if ( current->is_leaf ) {
		if ( idx->idx_schema->is_unique ) {
			bool found = true;
			found = dbidx_find_record(idx->idx_schema, current, key) != NULL;
			if ( found )
				return rv;
		}
		dbidx_add_node_value(idx->idx_schema, current, key);
		rv = true;
	} else {
		index_order_t index = 0, i;

		/*
		check the middle-ish node to see if our id is higher or lower than that and
		determine if it'll be i++ or i--
		*/

		if ( current->num_children > 0 ) {
			i = current->num_children / 2;
			index = i;
			if ( dbidx_compare_keys(idx->idx_schema, current->children[i], key) < 0 ) {
				/*
				 * the current key is smaller than the new key,
				 * find the index where something larger than it is
				 * and use that index
				 * */
				for ( i++; i < current->num_children; i++ ) {
					index = i;
					if ( dbidx_compare_keys(idx->idx_schema, current->children[i], key) > 0 )
						break;
				}
			} else {
				/*
				 * the current key is smaller than the new one, so count down until it is
				 * count down until we find the point where decreasing the index would make the
				 * new key smller than the current and break before we set that index
				 */
				do {
					i--;
					if ( dbidx_compare_keys(idx->idx_schema, current->children[i], key) < 0 )
						break;
					index = i;
				} while ( i > 0 );
			}
		}
		dbidx_add_index_value(idx, current->children[index]->childnode, key);
	}
	return rv;
}

bool dbidx_remove_index_value (db_index_t *idx, db_idxnode_t *idxnode, db_indexkey_t *key) {
	if ( idxnode == NULL )
		idxnode = idx->root_node;

	char msg[128];
	idx_key_to_str(idx->idx_schema, key, msg);
	printf("Removing %s(%" PRIu64 ")\n", msg, key->record);

	db_idxnode_t *leaf_node = dbidx_find_node(idx->idx_schema, idxnode, key);

	bool success = dbidx_remove_node_value(idx->idx_schema, leaf_node, key);

	if ( idxnode == idxnode->parent )
		dbidx_collapse_nodes(idx->idx_schema, idxnode);

	return success;
}

db_idxnode_t *dbidx_add_node_value(db_index_schema_t *idx, db_idxnode_t *idxnode, db_indexkey_t *key) {
	if ( idxnode->num_children >= idx->index_order )
		idxnode = dbidx_split_node(idx, idxnode, key);

	index_order_t i = 0;
	for( i=0; i < idxnode->num_children && dbidx_compare_keys(idx, idxnode->children[i], key) < 0; i++ );

	if(i < idxnode->num_children) {
		memmove(idxnode->children + i + 1, idxnode->children + i, sizeof(char *) * (idxnode->num_children - i));
	} else if ( i == idxnode->num_children ) {
		dbidx_update_max_value(idxnode->parent, idxnode, key);
	}

	(idxnode->num_children)++;

	/*
		new keys for leaf nodes are externally allocated and must be copied
		node keys are internally allocated and can be pointed to immeediately and will
		be released properly when cleaned up
	*/
	if ( !idxnode->is_leaf ) {
		idxnode->children[i] = key;
	} else {
		db_indexkey_t *new_key = dbidx_allocate_key(idx);
		dbidx_copy_key(key, new_key);
		idxnode->children[i] = new_key;
	}

	return idxnode;
}

bool dbidx_remove_node_value(db_index_schema_t *idx, db_idxnode_t *idxnode, db_indexkey_t *key) {
	db_indexkey_t *v;
	//char msg[128];
	bool success = false;
	int merge_amt = idx->index_order / 2;

	while ( dbidx_compare_keys(idx, idxnode->children[0], key) <= 0 ) {
		for ( index_order_t i = 0; i < idxnode->num_children; i++ ) {
			if ( dbidx_compare_keys(idx, idxnode->children[i], key) == 0 ) {
				v = idxnode->children[i];

				for ( index_order_t k = i+1; k < idxnode->num_children; k++)
					idxnode->children[k-1] = idxnode->children[k];

				(idxnode->num_children)--;
				idxnode->children[idxnode->num_children] = 0;
				free(v);

				if ( idxnode->num_children > 0 && idxnode->num_children <= merge_amt ) {
					int free_count = 0;

					if ( idxnode->prev != NULL )
						free_count += idx->index_order - idxnode->prev->num_children;
					if ( idxnode->next != NULL )
						free_count += idx->index_order - idxnode->next->num_children;

					if ( free_count > idxnode->num_children ) {
						int move_left, move_right, free_left, free_right;
						db_idxnode_t *c;

						free_left = idxnode->prev == NULL ? 0 : idx->index_order - idxnode->prev->num_children;
						free_right = idxnode->next == NULL ? 0 : idx->index_order - idxnode->next->num_children;

						if ( free_left > free_right ) {
							move_right =  idxnode->num_children / 2;
							move_left = idxnode->num_children - move_right;

						} else {
							move_left =  idxnode->num_children / 2;
							move_right = idxnode->num_children - move_left;

						}

						while ( free_left < move_left || free_right < move_right ) {
							if ( free_left < move_left ) {
								move_left--;
								move_right++;
							} else {
								move_left++;
								move_right--;
							}
						}

						if ( move_right > 0 && idxnode->next != NULL ) {
							c = idxnode->next;
							memmove(c->children + move_right, c->children, sizeof(char *) * c->num_children);
							for ( int k = 0; k < move_right; k++ ) {
								c->children[k] = idxnode->children[move_left + k];
								if ( !c->is_leaf )
									c->children[k]->childnode->parent = c;

								(c->num_children)++;
								idxnode->children[move_left + k] = 0;
							}
						}

						if ( move_left > 0 && idxnode->prev != NULL ) {
							c = idxnode->prev;
							for ( int k = 0; k < move_left; k++ ) {
								c->children[c->num_children] = idxnode->children[k];
								if ( !c->is_leaf )
									c->children[c->num_children]->childnode->parent = c;

								(c->num_children)++;
								idxnode->children[k] = 0;
							}
							dbidx_update_max_value(c->parent, c, c->children[c->num_children-1]);
						}

						(idxnode->num_children) -= move_right;
						(idxnode->num_children) -= move_left;
						/*
						Figure how much may be distrubted left (if any) and right (if any)
						Attempt to weight the distribution to the 'more empty' side
						*/
						//memmove(idxnode->children + i + 1, idxnode->children + i, sizeof(void *) * (idxnode->num_children - i));
					}
				}

				if ( idxnode != idxnode->parent ) {
					if ( idxnode->num_children == 0 ) {
						if ( idxnode-> prev != NULL )
							idxnode->prev->next = idxnode->next;

						if ( idxnode->next != NULL )
							idxnode->next->prev = idxnode->prev;

						for ( index_order_t k=0; k < idxnode->parent->num_children; k++) {
							if ( idxnode->parent->children[k]->childnode == idxnode ) {
								dbidx_remove_node_value(idx, idxnode->parent, idxnode->parent->children[k]);
								break;
							}
						}
						free(idxnode);
					} else {
						if (i == idxnode->num_children ) {
							dbidx_update_max_value(idxnode->parent, idxnode, idxnode->children[i-1]);
						}
					}
				}

				success = true;
				break;
			}
		}

		if ( success ) {
			break;

		} else if ( idxnode->next == NULL ) {
			break;

		} else {
			idxnode = idxnode->next;

		}
	}

	return success;
}

db_idxnode_t *dbidx_split_node(db_index_schema_t *idx, db_idxnode_t *idxnode, db_indexkey_t *key) {
	index_order_t nc = idxnode->num_children / 2;
	db_idxnode_t *rv = NULL;

	// determine if the new value causing the split will be on the right or left side of the split
	// and make that node slightly emptier
	if ( dbidx_compare_keys(idx, idxnode->children[nc], key) < 0 )
		nc++;

	if ( idxnode->parent != idxnode ) {
		// check lower range for availability, if we can locate there
		if ( idxnode->prev != NULL && idxnode->is_leaf && dbidx_compare_keys(idx, idxnode->children[0], key) > 0 )
			if ( idxnode->prev->num_children < idx->index_order )
				return idxnode->prev;

		// split node if there is no room
		if ( idxnode->parent->num_children >= idx->index_order )
			dbidx_split_node(idx, idxnode->parent, key);

		db_idxnode_t *new_node, *child_node;
		new_node = dbidx_allocate_node(idx);
		db_indexkey_t *new_k = dbidx_allocate_key(idx);

		new_node->is_leaf = idxnode->is_leaf;
		new_node->parent = idxnode->parent;
		new_node->prev = idxnode->prev;
		if ( new_node->prev != NULL )
			new_node->prev->next = new_node;

		new_node->next = idxnode;
		idxnode->prev = new_node;

		for(int i=0; i<nc; i++) {
			if( !new_node->is_leaf ) {
				child_node = idxnode->children[i]->childnode;
				child_node->parent = new_node;
			}
			new_node->children[i] = idxnode->children[i];
			new_node->num_children++;
		}

		dbidx_copy_key(new_node->children[new_node->num_children-1], new_k);
		new_k->childnode = new_node;

		dbidx_add_node_value(idx, idxnode->parent, new_k);

		for(int i=0; i<idxnode->num_children - nc; i++) {
			idxnode->children[i] = idxnode->children[nc+i];
			idxnode->children[nc+i] = 0;
		}

		idxnode->num_children -= nc;

		if ( dbidx_compare_keys(idx, new_node->children[new_node->num_children - 1], key) < 0 &&
				dbidx_compare_keys(idx, idxnode->children[0], key) > 0 ) {
			if ( new_node->num_children >= idxnode->num_children ) {
				rv = idxnode;
			} else {
				rv = new_node;
			}

		} else if ( dbidx_compare_keys(idx, new_node->children[new_node->num_children - 1], key) > 0 ) {
			rv = new_node;

		} else {
			rv = idxnode;

		}

	} else if ( idxnode->parent == idxnode ) {
		/* special case for the root node */
		db_idxnode_t *new_left, *new_right, *child_node;
		db_indexkey_t *new_left_k, *new_right_k;
		db_indexkey_t *old_left_k = NULL, *old_right_k = NULL;

		new_left = dbidx_allocate_node(idx);
		new_right = dbidx_allocate_node(idx);

		new_left_k = dbidx_allocate_key(idx);
		new_right_k = dbidx_allocate_key(idx);

		/* fix the new left node */
		new_left->is_leaf = idxnode->is_leaf;
		new_left->parent = idxnode;
		for(int i=0; i < nc; i++) {
			if( !new_left->is_leaf ) {
				child_node = idxnode->children[i]->childnode;
				child_node->parent = new_left;
			}
			new_left->children[i] = idxnode->children[i];
			old_left_k = new_left->children[i];
			idxnode->children[i] = NULL;
			new_left->num_children++;
		}

		dbidx_copy_key(old_left_k, new_left_k);
		new_left_k->childnode = new_left;

		/* fix the new right node */
		new_right->is_leaf = idxnode->is_leaf;
		new_right->parent = idxnode;
		for(int i=nc; i < idxnode->num_children; i++) {
			if( !new_right->is_leaf ) {
				child_node = idxnode->children[i]->childnode;
				child_node->parent = new_right;
			}
			new_right->children[i - nc] = idxnode->children[i];
			old_right_k = new_right->children[i - nc];
			idxnode->children[i] = NULL;
			new_right->num_children++;
		}

		dbidx_copy_key(old_right_k, new_right_k);
		new_right_k->childnode = new_right;

		idxnode->is_leaf = false;
		idxnode->num_children = 2;
		idxnode->children[0] = new_left_k;
		idxnode->children[1] = new_right_k;

		new_left->next = new_right;
		new_right->prev = new_left;

		if ( dbidx_compare_keys(idx, new_left->children[new_left->num_children - 1], key) < 0 &&
				dbidx_compare_keys(idx, new_right->children[0], key) > 0 ) {
			if ( new_left->num_children >= new_right->num_children ) {
				rv = new_right;
			} else {
				rv = new_left;
			}
		} else if ( dbidx_compare_keys(idx, new_left->children[new_left->num_children - 1], key) > 0 ) {
			rv = new_left;
		} else {
			rv = new_right;
		}
	}

	return rv;
}

void dbidx_collapse_nodes(db_index_schema_t *idx, db_idxnode_t *idxnode) {
	if ( idxnode->is_leaf )
			return;

	index_order_t nc = dbidx_num_child_records(idxnode);

	//should this be < or <=?
	if ( nc <= idx->index_order && nc > 0 ) {

		db_idxnode_t *cn = idxnode->children[0]->childnode;
		db_indexkey_t *children[idx->index_order];
		int index = 0;

		idxnode->is_leaf = cn->is_leaf;

		for(index_order_t i = 0; i < idxnode->num_children; i++) {
			cn = idxnode->children[i]->childnode;
			for (index_order_t k = 0; k < cn->num_children; k++) {
				children[index] = cn->children[k];
				if ( !idxnode->is_leaf )
					children[index]->childnode->parent = idxnode;
				index++;
			}
			free(cn);
		}

		for(index_order_t i = 0; i < idx->index_order; i++) {
			if ( i < idxnode->num_children )
				free(idxnode->children[i]);
			idxnode->children[i] = NULL;
		}
		idxnode->num_children = 0;

		for(int i=0; i < index; i++) {
			(idxnode->num_children)++;
			idxnode->children[i] = children[i];
		}
	}
}

void dbidx_update_max_value (db_idxnode_t *parent_idx, db_idxnode_t *idxnode, db_indexkey_t *new_key) {
	db_indexkey_t *current_key = NULL;
	int i = 0;

	if ( (char *)parent_idx != (char *)idxnode ) {
		for(i=0; i < parent_idx->num_children; i++) {
			current_key = parent_idx->children[i];
			if ( current_key->childnode == idxnode ) {
				dbidx_copy_key(new_key, current_key);
				current_key->childnode = idxnode;
				break;
			}
		}
		if ( i == parent_idx->num_children - 1)
			dbidx_update_max_value(parent_idx->parent, parent_idx, new_key);
	}
}

void dbidx_print_tree(db_index_t *idx, db_idxnode_t *idxnode, uint64_t *counter) {
	db_idxnode_t *s = idxnode, *starting_node;
	if ( s == NULL ) {
		printf("Printing index %s\n", idx->index_name);
		s = idx->root_node;
	}
	int page = 1;
	starting_node = s;

	printf("Level %" PRIu64 ": ", *counter);
	do {
		for(index_order_t i = 0; i < s->num_children; i++) {
			// This is hardcoded and shouldn't be
			char strkey[128];
			idx_key_to_str(idx->idx_schema, s->children[i], strkey);
			printf("%s%s(%" PRIu64 ") (%s %d)", page == 1 && i == 0 ? "" : " ", strkey, s->children[i]->record, s->is_leaf ? "leaf" : "node", page);
		}
		s = s->next;
		page++;
	} while ( s != NULL );
	printf("\n");

	(*counter)++;
	if ( !starting_node->is_leaf )
		dbidx_print_tree(idx, starting_node->children[0]->childnode, counter);
}

void dbidx_print_tree_totals(db_index_t *idx, db_idxnode_t *idxnode, uint64_t *counter) {
	db_idxnode_t *s = idxnode, *starting_node;
	uint64_t children = 0;

	if ( s == NULL )
		s = idx->root_node;
	starting_node = s;

	do {
		children += s->num_children;
		s = s->next;
	} while ( s != NULL );
	printf("Level %" PRIu64 ": %" PRIu64 " children\n", *counter, children);

	(*counter)++;
	if ( !starting_node->is_leaf )
		dbidx_print_tree_totals(idx, starting_node->children[0]->childnode, counter);
}

void dbidx_print_index_scan_lookup(db_index_t *idx, db_indexkey_t *key) {
	printf("Performing an index scan using %s\n", idx->index_name);
	db_idxnode_t *i = idx->root_node;
	while ( !i->is_leaf )
		i = i->children[0]->childnode;

	if ( i->num_children == 0 )
		return;

	char left[128], right[128], msg[128];
	idx_key_to_str(idx->idx_schema, i->children[0], left);
	idx_key_to_str(idx->idx_schema, i->children[i->num_children-1], right);

	printf("Starting with %p (%s -> %s)\n",
			(void *)i,
			left,
			right);
	index_order_t index = INDEX_ORDER_MAX;
	uint64_t leafcounter = 0;

	do {
		leafcounter++;
		for ( index = 0; index < i->num_children; index++ )
			if ( dbidx_compare_keys(idx->idx_schema, i->children[index], key) >= 0 )
				break;

		if ( index < i->num_children )
			break;
		else
			i = i->next;
	} while ( i != NULL );

	if ( i !=  NULL &&
			index < i->num_children &&
			dbidx_compare_keys(idx->idx_schema, i->children[index], key) == 0) {

		idx_key_to_str(idx->idx_schema, i->children[index], msg);
		printf("Found key %s on %p in index %d %" PRIu64 " leaves in\n",
				msg,
				(void *)i,
				index,
				leafcounter);
		for( ;; ) {
			idx_key_to_str(idx->idx_schema, i->children[0], left);
			idx_key_to_str(idx->idx_schema, i->children[i->num_children-1], right);

			printf("%p %s (%s -> %s)\n",
					(void *)i,
					i == i->parent ? "root" : i->is_leaf ? "leaf" : "node",
					left,
					right
					);
			if ( i == i->parent )
				break;
			i = i->parent;
		}
	} else {
		idx_key_to_str(idx->idx_schema, key, msg);
		printf("Key %s could not be found in %" PRIu64 " leaves\n", msg, leafcounter);
	}
}

/*
void dbidx_read_index_from_file(db_index_t *idx) {
	char *ipth;
	if ( (ipth = getenv("TABLE_DATA")) == NULL )
		ipth = DEFAULT_BASE;

	int fd = -1;
	size_t sz = strlen(ipth) + 1 + strlen(idx->index_name) + 5;

	// path + '/' + name + '.idx' + \0
	char *idxfile = malloc(sz);
	bzero(idxfile, sz);

	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idx");

	printf("Reading keys from file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		fprintf(stderr, "Error: %s\n", strerror(errno));
	}
	if ( (fd = open(idxfile, O_RDONLY, 0640)) >= 0 ) {
		uint16_t recordsize = 0;
		uint64_t recordcount = 0;
		uint64_t counter = 0;

		if ( read(fd, &recordsize, sizeof(uint16_t)) != sizeof(uint16_t) ) {
			printf("Incomplete read of recordsize\n");
		}
		if ( read(fd, &recordcount, sizeof(uint64_t)) != sizeof(uint64_t) ) {
			printf("Incomplete read of recordcount\n");
		}
		printf("%" PRIu64 " records of size %"PRIu16" exist in index file\n", recordcount, recordsize);
		void *buff = malloc(sizeof(char)*recordsize);
		for(counter = 0; counter < recordcount; counter++) {
			bzero(buff, sizeof(char)*recordsize);
			if ( read(fd, buff, recordsize) != recordsize ) {
				// an index needs to be remapped in some way
			} else {
				add_index_value(idx, &idx->root_node, buff);
			}
		}
		free(buff);
	} else
		fprintf(stderr, "Error: %s\n", strerror(errno));

	free(idxfile);
}
*/

void dbidx_write_file_records(db_index_t *idx) {
	int fd = -1;
	char *ipth;

	if ( (ipth = getenv("TABLE_DATA")) == NULL )
		ipth = DEFAULT_BASE;

	size_t sz = strlen(ipth) + 1 + strlen(idx->index_name) + 5;

	// path + '/' + name + '.idx' + \0
	char *idxfile = malloc(sz);
	bzero(idxfile, sz);

	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idx");

	printf("Writing records to file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		fprintf(stderr, "Error: %s\n", strerror(errno));
	}

	if ( (fd = open(idxfile, O_CREAT | O_RDWR | O_TRUNC, 0640)) >= 0 ) {
		db_idxnode_t *cn = idx->root_node;

		while(!cn->is_leaf)
			cn = cn->children[0]->childnode;

		uint64_t recordcount = 0;

		write(fd, &recordcount, sizeof(recordcount));
		while ( cn != NULL ) {
			for(i = 0; i < cn->num_children; i++) {
				write(fd, &cn->children[i]->record, sizeof(uint64_t));
				recordcount++;
			}
			cn = cn->next;
		}
		lseek(fd, 0, SEEK_SET);
		write(fd, &recordcount, sizeof(recordcount));
		close(fd);

		printf("%" PRIu64 " records of size %lu written\n", recordcount, sizeof(uint64_t));
	}
	free(idxfile);
}

void dbidx_write_file_keys(db_index_t *idx) {
	int fd = -1;
	char *ipth;

	if ( (ipth = getenv("TABLE_DATA")) == NULL )
		ipth = DEFAULT_BASE;

	size_t sz = strlen(ipth) + 1 + strlen(idx->index_name) + 9;

	// path + '/' + name + '.idx' + \0
	char *idxfile = malloc(sz);
	bzero(idxfile, sz);

	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idxkeys");

	printf("Writing keys and records to file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		fprintf(stderr, "Error: %s\n", strerror(errno));
	}

	if ( (fd = open(idxfile, O_CREAT | O_RDWR | O_TRUNC, 0640)) >= 0 ) {
		db_idxnode_t *cn = idx->root_node;
		uint64_t header = 0;

		db_index_schema_t *schema = idx->idx_schema;
		write(fd, &schema->num_fields, sizeof(uint8_t));
		header += sizeof(uint8_t);
		printf("%ld bytes\n", sizeof(uint8_t));
		for(uint8_t i = 0; i < schema->num_fields; i++) {
			dd_datafield_t *f = schema->fields[i];
			write(fd, f, sizeof(dd_datafield_t));
			header += sizeof(dd_datafield_t);
			printf("%ld bytes\n", sizeof(dd_datafield_t));
		}

		while(!cn->is_leaf)
			cn = cn->children[0]->childnode;

		uint64_t recordcount = 0;

		write(fd, &recordcount, sizeof(recordcount));
		while ( cn != NULL ) {
			for(i = 0; i < cn->num_children; i++) {
				printf("writing key %s size %u\n", *cn->children[i]->data, schema->record_size);
				write(fd, *cn->children[i]->data, schema->record_size);
				write(fd, &cn->children[i]->record, sizeof(uint64_t));
				recordcount++;
			}
			cn = cn->next;
		}
		lseek(fd, header, SEEK_SET);
		printf("Seeking back %" PRIu64 " bytes\n", header);
		write(fd, &recordcount, sizeof(recordcount));
		close(fd);

		printf("%" PRIu64 " records of size %u written\n", recordcount, schema->record_size);
	}
	free(idxfile);
}
