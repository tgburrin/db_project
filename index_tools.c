#include "index_tools.h"

void init_index_node(idxnode_t *idxnode) {
	bzero(idxnode, sizeof(idxnode_t));
	idxnode->num_children = 0;
	idxnode->is_leaf = true;
	idxnode->parent = (struct idxnode_t *)idxnode;
	idxnode->next = 0;
	idxnode->prev = 0;
}

int num_child_records(index_t *idx, idxnode_t *idxnode) {
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

int find_node_index(index_t *idx, idxnode_t *idxnode, void *find_rec, int *index) {
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

idxnode_t *find_node(index_t *idx, idxnode_t *idxnode, void *find_rec) {
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

void *find_record(index_t *idx, idxnode_t *idxnode, void *find_rec) {
		void *rv = NULL;

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
			rv = current->children[index];

		return rv;
}

idxnode_t *split_node(index_t *idx, idxnode_t *idxnode, void *key) {
	uint16_t nc = idxnode->num_children / 2;
	idxnode_t *rv = 0;

	// determine if the new value causing the split will be on the right or left side of the split
	// and make that node slightly emptier
	if ( (*idx->compare_key)(idxnode->children[nc], key) < 0 )
		nc++;

	if ( (void *)idxnode->parent != (void *)idxnode ) {
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

	} else if ( (void *)idxnode->parent == (void *)idxnode ) {
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

void update_max_value (index_t *idx, idxnode_t *parent_idx, idxnode_t *idxnode, void *new_key) {
	indexkey_t *current_key;
	int i = 0;

	if ( (void *)parent_idx != (void *)idxnode ) {
		for(i=0; i < parent_idx->num_children; i++) {
			current_key = parent_idx->children[i];
			if ( ((indexkey_t *)current_key)->childnode == idxnode ) {
				(*idx->copy_key)(new_key, current_key);
				((indexkey_t *)current_key)->childnode = idxnode;
				break;
			}
		}
		if ( i == parent_idx->num_children - 1)
			update_max_value(idx, (idxnode_t *)parent_idx->parent, (idxnode_t *)parent_idx, new_key);
	}
}

idxnode_t *add_node_value (index_t *idx,idxnode_t *idxnode, void *key) {

	if ( idxnode->num_children >= IDX_ORDER )
		idxnode = split_node(idx, idxnode, key);

 	int i = 0;
	for( i=0; i < idxnode->num_children && (*idx->compare_key)(idxnode->children[i], key) < 0; i++ );

	if(i < idxnode->num_children) {
		memmove(idxnode->children + i + 1, idxnode->children + i, sizeof(void *) * (idxnode->num_children - i));
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
		indexkey_t *new_key = (*idx->create_key)();
		(*idx->copy_key)(key, new_key);
		idxnode->children[i] = new_key;
	}

	return idxnode;
}

bool add_index_value (index_t *idx, idxnode_t *idxnode, void *key) {
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

void collapse_nodes(index_t *idx, idxnode_t *idxnode) {
		if ( idxnode->is_leaf )
				return;

		int nc = num_child_records(idx, idxnode);

		//should this be < or <=?
		if ( nc <= IDX_ORDER && nc > 0 ) { //&& idxnode->next == NULL && idxnode->prev == NULL ) {
				//printf("Collapsing singular node\n");

				idxnode_t *cn = ((indexkey_t *)(idxnode->children[0]))->childnode;
				void *children[IDX_ORDER];
				int index = 0;

				idxnode->is_leaf = cn->is_leaf;

				for(int i=0; i < idxnode->num_children; i++) {
						cn = ((indexkey_t *)(idxnode->children[i]))->childnode;
						for (int k=0; k < cn->num_children; k++) {
								children[index] = cn->children[k];
								//printf("storing reference to %s\n", ((idxorderkey_t *)children[index])->orderid);
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

bool remove_node_value(index_t *idx, idxnode_t *idxnode, void *key) {
	void *v;
	//char msg[128];
	bool success = false;
	int merge_amt = IDX_ORDER / 2;

	while ( (*idx->compare_key)(idxnode->children[0], key) <= 0 ) {
		for ( int i = 0; i < idxnode->num_children; i++ ) {
			if ( (*idx->compare_key)(idxnode->children[i], key) == 0 ) {
				//(*idx->print_key)(idxnode->children[i], msg);
				//printf("Located key %s in index %d\n", msg, i);
				v = idxnode->children[i];

				for ( int k=i+1; k < idxnode->num_children; k++)
					idxnode->children[k-1] = idxnode->children[k];

				(idxnode->num_children)--;
				idxnode->children[idxnode->num_children] = 0;
				free(v);

				if ( idxnode->num_children > 0 && idxnode->num_children <= merge_amt ) {
					//printf("Attempting merge of nodes\n");
					int free_count = 0;

					if ( idxnode->prev != NULL )
						free_count += IDX_ORDER - ((idxnode_t *)idxnode->prev)->num_children;
					if ( idxnode->next != NULL )
						free_count += IDX_ORDER - ((idxnode_t *)idxnode->next)->num_children;

					if ( free_count > idxnode->num_children ) {
						//printf("merge is possible\n");
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

						//printf("Moving\n\t%d <- | -> %d\n", move_left, move_right);

						if ( move_right > 0 && idxnode->next != NULL ) {
							//printf("Moving %d keys right\n", move_right);
							c = (idxnode_t *)idxnode->next;
							memmove(c->children + move_right, c->children, sizeof(void *) * c->num_children);
							for ( int k = 0; k < move_right; k++ ) {
								c->children[k] = idxnode->children[move_left + k];
								if ( !c->is_leaf )
									((idxnode_t *)(((indexkey_t *)(c->children[k])))->childnode)->parent = (struct idxnode_t *)c;

								(c->num_children)++;
								idxnode->children[move_left + k] = 0;
							}
						}

						if ( move_left > 0 && idxnode->prev != NULL ) {
							//printf("Moving %d keys left\n", move_left);
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

				if ( (void *)idxnode != (void *)idxnode->parent ) {
					if ( idxnode->num_children == 0 ) {
						//printf("%s is empty,\n\tfixing prev/next pointers\n", idxnode->is_leaf ? "Leaf" : "Node");
						if ( idxnode-> prev != NULL )
							((idxnode_t *)idxnode->prev)->next = idxnode->next;

						if ( idxnode->next != NULL )
							((idxnode_t *)idxnode->next)->prev = idxnode->prev;

						//printf("\tremoving key from parent\n");
						for ( int k=0; k < ((idxnode_t *)idxnode->parent)->num_children; k++) {
							//printf("Checking parent (%p) key in slot %d\n", idxnode->parent, k);
							if ( ((indexkey_t *)(((idxnode_t *)idxnode->parent)->children[k]))->childnode == idxnode ) {
								//printf("Located parent record\n");
								remove_node_value(idx, (idxnode_t *)idxnode->parent, ((idxnode_t *)idxnode->parent)->children[k]);
								break;
							}
						}
						//printf("\tfreeing %s\n", idxnode->is_leaf ? "leaf" : "node");
						free(idxnode);
					} else {
						if (i == idxnode->num_children ) {
							//printf("Leaf node max has changed, updating parent\n");
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

bool remove_index_value (index_t *idx, idxnode_t *idxnode, void *key) {
		idxnode_t *leaf_node = find_node(idx, idxnode, key);

		bool success = remove_node_value(idx, leaf_node, key);

		if ( (void *)idxnode == (void *)idxnode->parent )
				collapse_nodes(idx, idxnode);

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

void print_index_scan_lookup(index_t *idx, void *key) {
	printf("Performing an index scan\n");
	idxnode_t *i = &idx->root_node;
	while ( !i->is_leaf )
		i = ((indexkey_t *)i->children[0])->childnode;

	char left[128], right[128], msg[128];
	(*idx->print_key)(i->children[0], left);
	(*idx->print_key)(i->children[i->num_children-1], right);

	printf("Starting with %p (%s -> %s)\n",
			i,
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
				i,
				index,
				leafcounter);
		for( ;; ) {
			(*idx->print_key)(i->children[0], left);
			(*idx->print_key)(i->children[i->num_children-1], right);

			printf("%p %s (%s -> %s)\n",
					i,
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
