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
						rv += ((idxnode_t *)(*idx->get_key_value)(idxnode->children[i]))->num_children;
		} else {
				rv = idxnode->num_children;
		}
		return rv;
}

idxnode_t *find_node(index_t *idx, idxnode_t *idxnode, void *find_rec) {
		if ( idxnode->is_leaf )
				return idxnode;

		int index = 0, i = idxnode->num_children / 2;
		if ( (*idx->compare_key)(idxnode->children[i], find_rec) < 0 ) {
				//printf("greater than midpoint\n");
				for ( ; i < idxnode->num_children; i++ ) {
						index = i;
						if ( (*idx->compare_key)(idxnode->children[i], find_rec) > 0 )
								break;
				}
		} else {
				//printf("less than midpoint\n");
				for ( ; i >= 0; --i ) {
						if ( (*idx->compare_key)(idxnode->children[i], find_rec) < 0 )
								break;
						index = i;
				}
		}
		//printf("Descending to %p from key %s\n",
		//	  ((idxorderkey_t *)idxnode->children[index])->record,
		//	  ((idxorderkey_t *)idxnode->children[index])->orderid);
		return find_node(idx, (*idx->get_key_value)(idxnode->children[index]), find_rec);
}

void *find_record(index_t *idx, idxnode_t *idxnode, void *find_rec) {
		char msg[64];
		void *rv = NULL;

		(*idx->print_key)(find_rec, msg);
		//printf("Finding record %s\n", msg);

		idxnode = find_node(idx, idxnode, find_rec);
		int index = 0, i;

		// edge case, empty index
		if ( idxnode->num_children == 0 )
			return rv;

		i = idxnode->num_children / 2;
		if ( (*idx->compare_key)(idxnode->children[i], find_rec) < 0 ) {
				//printf("greater than midpoint\n");
				for ( ; i < idxnode->num_children; i++ ) {
						index = i;
						if ( (*idx->compare_key)(idxnode->children[i], find_rec) > 0 )
								break;
				}
		} else {
				//printf("less than midpoint\n");
				for ( ; i >= 0; --i ) {
						if ( (*idx->compare_key)(idxnode->children[i], find_rec) < 0 )
								break;
						index = i;
				}
		}

		//printf("Taking action on child %d\n", index);
		if ( (*idx->compare_key)(idxnode->children[index], find_rec) == 0 ) {
			// the record value could be comparied here for non-unique keys
			//rv = (*idx->get_key_value)(idxnode->children[index]);
			rv = idxnode->children[index];
			//(*idx->print_key)(rv, msg);
			//printf("Located %s on %p\n", msg, rv);
		} else if ( i == idxnode->num_children && idxnode->next != NULL ) {
			rv = find_record(idx, (idxnode_t *)idxnode->next, find_rec);
		/*
		} else {
			(*idx->print_key)(find_rec, msg);
			printf("Record %s not found\n", msg);
		*/
		}
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
		(*idx->copy_key)(key, new_k);

		new_node->is_leaf = idxnode->is_leaf;
		new_node->parent = idxnode->parent;
		new_node->prev = idxnode->prev;
		if ( new_node->prev != NULL )
			((idxnode_t *)new_node->prev)->next = (struct idxnode_t *)new_node;

		new_node->next = (struct idxnode_t *)idxnode;
		idxnode->prev = (struct idxnode_t *)new_node;

		for(int i=0; i<nc; i++) {
			if( !new_node->is_leaf ) {
				child_node = (idxnode_t *)((indexkey_t *)idxnode->children[i])->record;
				child_node->parent = (struct idxnode_t *)new_node;
			}
			new_node->children[i] = idxnode->children[i];
			new_node->num_children++;
		}

		(*idx->set_key_value)(new_k, new_node);

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
				child_node = (idxnode_t *)((*idx->get_key_value)(idxnode->children[i]));
				child_node->parent = (struct idxnode_t *)new_left;
			}
			new_left->children[i] = idxnode->children[i];
			old_left_k = new_left->children[i];
			idxnode->children[i] = 0;
			new_left->num_children++;
		}

		(*idx->copy_key)(old_left_k, new_left_k);
		(*idx->set_key_value)(new_left_k, new_left);

		new_right->is_leaf = idxnode->is_leaf;
		new_right->parent = (struct idxnode_t *)idxnode;
		for(int i=nc; i < idxnode->num_children; i++) {
			if( !new_right->is_leaf ) {
				child_node = (idxnode_t *)((*idx->get_key_value)(idxnode->children[i]));
				child_node->parent = (struct idxnode_t *)new_right;
			}
			new_right->children[i - nc] = idxnode->children[i];
			old_right_k = new_right->children[i - nc];
			idxnode->children[i] = 0;
			new_right->num_children++;
		}

		(*idx->copy_key)(old_right_k, new_right_k);
		(*idx->set_key_value)(new_right_k, new_right);

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
			if ( (*idx->get_key_value)(current_key) == idxnode ) {
				(*idx->copy_key)(new_key, current_key);
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
		new order keys for leaf nodes are externally allocated and must be copied
		node keys are internally allocated and can be pointed to immeediately and will
		be released properly when cleaned up
	*/
	if ( !idxnode->is_leaf ) {
		idxnode->children[i] = key;
	} else {
		indexkey_t *new_key = (*idx->create_key)();
		(*idx->copy_key)(key, new_key);
		(*idx->set_key_value)(new_key, (*idx->get_key_value)(key));
		idxnode->children[i] = new_key;
	}

	return idxnode;
}

bool add_index_value (index_t *idx, idxnode_t *idxnode, void *key) {
	bool rv = false;
	if ( (void *)idxnode->parent == (void *)idxnode ) {
		char strkey[64];
		(*idx->print_key)(key, strkey);

	}

	if ( idxnode->is_leaf ) {
		if ( idx->is_unique ) {
			bool found = true;
			// store the current value
			void *value = (*idx->get_key_value)(key);

			// replace it with a wildcard search value
			(*idx->set_key_value)(key, (void *)-1);

			found =  find_record(idx, idxnode, key) != NULL;

			(*idx->set_key_value)(key, value);
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

		add_index_value(idx, (*idx->get_key_value)(idxnode->children[index]), key);
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

				idxnode_t *cn = (*idx->get_key_value)(idxnode->children[0]);
				void *children[IDX_ORDER];
				int index = 0;

				idxnode->is_leaf = cn->is_leaf;

				for(int i=0; i < idxnode->num_children; i++) {
						cn = (*idx->get_key_value)(idxnode->children[i]);
						for (int k=0; k < cn->num_children; k++) {
								children[index] = cn->children[k];
								//printf("storing reference to %s\n", ((idxorderkey_t *)children[index])->orderid);
								if ( !idxnode->is_leaf )
										((idxnode_t *)(*idx->get_key_value)(children[index]))->parent = (struct idxnode_t *)idxnode;
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
	bool success = false;
	int merge_amt = IDX_ORDER / 2;

	while ( (*idx->compare_key)(idxnode->children[0], key) <= 0 ) {
		//printf("Checking %s %p values for search value %s\n",
		//		idxnode->is_leaf ? "leaf" : "node",
		//		idxnode,
		//		key->orderid);
		for ( int i = 0; i < idxnode->num_children; i++ ) {
			if ( (*idx->compare_key)(idxnode->children[i], key) == 0 ) {
				v = idxnode->children[i];
				//printf("Located value %s (%p) on %s %p to be removed\n",
				//	key->orderid,
				//	v,
				//	idxnode->is_leaf ? "leaf" : "node",
				//	idxnode);

				for ( int k=i+1; k < idxnode->num_children; k++)
					idxnode->children[k-1] = idxnode->children[k];

				(idxnode->num_children)--;
				idxnode->children[idxnode->num_children] = 0;
				//printf("%d number of children left after key %s removed\n", idxnode->num_children, v->orderid);

				//printf("Releasing key %s (%p)\n", v->orderid, v);
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
									((idxnode_t *)((*idx->get_key_value)(c->children[k])))->parent = (struct idxnode_t *)c;

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
									((idxnode_t *)((*idx->get_key_value)(c->children[c->num_children])))->parent = (struct idxnode_t *)c;

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
							if ( (*idx->get_key_value)(((idxnode_t *)idxnode->parent)->children[k]) == idxnode ) {
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
	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idx");

	printf("Reading keys from file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		// handle error
	}
	if ( (fd = open(idxfile, O_RDONLY, 0640)) >= 0 ) {
		uint16_t recordsize = 0;
		uint64_t recordcount = 0;
		uint64_t counter = 0;

		if ( read(fd, &recordsize, sizeof(uint16_t)) != sizeof(uint16_t) ) {
			//error
		}
		if ( read(fd, &recordcount, sizeof(uint64_t)) != sizeof(uint64_t) ) {
			//error
		}
		printf("%" PRIu64 " records of size %"PRIu16" exist in index file\n", recordcount, recordsize);
		void *buff = malloc(sizeof(char)*recordsize);
		for(counter = 0; counter < recordcount; counter++) {
			bzero(buff, sizeof(char)*recordsize);
			if ( read(fd, buff, recordsize) != recordsize ) {
				//
			} else {
				add_index_value(idx, &idx->root_node, buff);
			}
		}
		free(buff);
	}

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
	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idx");

	printf("Writing keys to file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		// handle error
	}

	if ( (fd = open(idxfile, O_CREAT | O_RDWR | O_TRUNC, 0640)) >= 0 ) {
		idxnode_t *cn = &idx->root_node;

		while(!cn->is_leaf)
			cn = (idxnode_t *)((*idx->get_key_value)(cn->children[0]));

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
		char msg[64];
		(*idx->print_key)(key, msg);

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
			idxnode_t *c = (idxnode_t *)((*idx->get_key_value)(idxnode->children[i]));
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
		print_tree(idx, (*idx->get_key_value)(s->children[0]), counter);
}
