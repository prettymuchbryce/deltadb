package db

import "errors"

func (appendix *appendix) applyOperation(operation *Operation) error {
	switch operation.Type {
	case OpCreateCollection:
		return createCollection(appendix, operation)
	case OpDeleteCollection:
		return deleteCollection(appendix, operation)
	case OpSetDocument:
		return setDocument(appendix, operation)
	case OpDeleteDocument:
		return deleteDocument(appendix, operation)
	case OpRollback:
		return rollback(appendix, operation)
	default:
		return nil
	}
}

func createCollection(appendix *appendix, operation *Operation) error {
	_, exists := appendix.Collections[operation.CollectionName]

	if exists {
		return errors.New("Collection already exists")
	}

	collectionName := operation.CollectionName

	collection := new(collection)
	collection.Name = collectionName
	collection.DocumentPointers = make(map[string]pointer)

	appendix.Collections[collectionName] = *collection

	return nil
}

func deleteCollection(appendix *appendix, operation *Operation) error {
	_, exists := appendix.Collections[operation.CollectionName]

	if !exists {
		return errors.New("Collection does not exist")
	}

	collectionName := operation.CollectionName

	delete(appendix.Collections, collectionName)

	return nil
}

func setDocument(appendix *appendix, operation *Operation) error {
	collectionName := operation.CollectionName
	id := operation.Id

	_, exists := appendix.Collections[collectionName]
	if !exists {
		return errors.New("Collection does not exist")
	}

	collection := appendix.Collections[collectionName]
	collection.DocumentPointers[id] = operation.Pointer

	return nil
}

func deleteDocument(appendix *appendix, operation *Operation) error {
	collectionName := operation.CollectionName
	id := operation.Id

	_, exists := appendix.Collections[collectionName]

	if !exists {
		return errors.New("Collection does not exist")
	}

	collection := appendix.Collections[collectionName]

	_, exists = collection.DocumentPointers[id]

	if !exists {
		return errors.New("Document does not exist")
	}

	delete(collection.DocumentPointers, id)

	return nil
}

func rollback(appendix *appendix, operation *Operation) error {
	hash := operation.Hash

	//First ensure that this commit exists
	if !appendix.CommitMap[hash] {
		return errors.New("Commit does not exist")
	}

	//Reset collection map
	appendix.Collections = make(map[string]collection)

	//Apply commits until we reach the target hash
	commitIndex := 0
	for {
		commit := appendix.Commits[commitIndex]
		appendix.applyOperation(&commit.Operation)
		if commit.Hash == hash {
			break
		}

		commitIndex++
	}

	return nil
}
