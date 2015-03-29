package db

import "errors"

func getHistoricalAppendix(context *Context, hash uint64) (error, appendix) {
	if !context.appendix.CommitMap[hash] {
		return errors.New("Commit does not exist"), context.appendix
	}

	historicalAppendix := *new(appendix)
	historicalAppendix.Commits = context.appendix.Commits
	historicalAppendix.CommitMap = context.appendix.CommitMap
	historicalAppendix.Collections = make(map[string]collection)

	rollbackOperation := new(Operation)
	rollbackOperation.Hash = hash
	rollbackOperation.Type = OpRollback

	rollback(&historicalAppendix, rollbackOperation)

	return nil, historicalAppendix
}

//Immutable & Passive Commands
func ListCollections(context *Context, hash uint64) (error, []string) {
	var appendix *appendix
	appendix = &context.appendix

	if hash != context.GetLatestHash() {
		err, historicalAppendix := getHistoricalAppendix(context, hash)
		if err != nil {
			return err, nil
		}
		appendix = &historicalAppendix
	}

	var collectionNames []string
	for i, _ := range appendix.Collections {
		collectionNames = append(collectionNames, appendix.Collections[i].Name)
	}

	return nil, collectionNames
}

func ListDocuments(context *Context, collectionName string, hash uint64) (error error, value []string) {
	var appendix *appendix
	appendix = &context.appendix

	if hash != context.GetLatestHash() {
		err, historicalAppendix := getHistoricalAppendix(context, hash)
		if err != nil {
			return err, nil
		}
		appendix = &historicalAppendix
	}

	_, exists := appendix.Collections[collectionName]
	if !exists {
		return errors.New("Collection does not exist"), nil
	}

	collection := appendix.Collections[collectionName]

	var documentIds []string
	for i, _ := range collection.DocumentPointers {
		documentIds = append(documentIds, i)
	}

	return nil, documentIds
}

func ListHistory(context *Context, limit int, hash uint64) (error, []Commit) {
	var appendix *appendix
	appendix = &context.appendix

	if hash != context.GetLatestHash() {
		err, historicalAppendix := getHistoricalAppendix(context, hash)
		if err != nil {
			return err, nil
		}
		appendix = &historicalAppendix
	}

	var history []Commit
	var max int

	if limit > len(appendix.Commits) {
		max = len(appendix.Commits)
	} else {
		max = limit
	}

	for i := 0; i < max; i++ {
		history = append(history, appendix.Commits[i])
	}

	return nil, history
}

func GetDocument(context *Context, collectionName string, id string, hash uint64) (error error, value string) {
	var appendix *appendix
	appendix = &context.appendix

	if hash != context.GetLatestHash() {
		err, historicalAppendix := getHistoricalAppendix(context, hash)
		if err != nil {
			return err, ""
		}
		appendix = &historicalAppendix
	}

	_, exists := appendix.Collections[collectionName]
	if !exists {
		return errors.New("Collection does not exist"), ""
	}

	collection := appendix.Collections[collectionName]

	_, exists = collection.DocumentPointers[id]
	if !exists {
		return errors.New("Document does not exist"), ""
	}

	pointer := collection.DocumentPointers[id]

	err, value := context.blobstore.getBlob(context, pointer)
	if err != nil {
		return err, ""
	}

	return nil, value
}

//Mutable & Historical Commands
func CreateCollection(context *Context, source string, message string, collectionName string) (uint64, error) {
	//create operation
	operation := new(Operation)
	operation.CollectionName = collectionName
	operation.Type = OpCreateCollection

	//apply operation to appendix
	hash, err := apply(context, source, message, operation)

	return hash, err
}

func DeleteCollection(context *Context, source string, message string, collectionName string) (uint64, error) {
	//create operation
	operation := new(Operation)
	operation.CollectionName = collectionName
	operation.Type = OpDeleteCollection

	//apply operation to appendix
	hash, err := apply(context, source, message, operation)

	return hash, err
}

func SetDocument(context *Context, source string, message string, collectionName string, id string, data string) (uint64, error) {
	//store blob
	pointer := context.blobstore.storeBlob(context, data)

	operation := new(Operation)
	operation.CollectionName = collectionName
	operation.Type = OpSetDocument
	operation.Id = id
	operation.Pointer = pointer

	hash, err := apply(context, source, message, operation)

	return hash, err
}

func DeleteDocument(context *Context, source string, message string, collectionName string, id string) (uint64, error) {
	operation := new(Operation)
	operation.CollectionName = collectionName
	operation.Type = OpDeleteDocument
	operation.Id = id
	operation.Pointer = ""

	hash, err := apply(context, source, message, operation)

	return hash, err
}

func Rollback(context *Context, source string, message string, hash uint64) (uint64, error) {
	operation := new(Operation)
	operation.Type = OpRollback
	operation.Hash = hash

	rollbackHash, err := apply(context, source, message, operation)

	return rollbackHash, err
}
func apply(context *Context, message string, source string, operation *Operation) (uint64, error) {
	//apply operation to appendix
	err := context.appendix.applyOperation(operation)
	if err != nil {
		return 0, err
	}

	//create commit
	err, commit := newCommit(message, source, *operation)
	if err != nil {
		return 0, err
	}

	//append commit
	context.appendix.Commits = append(context.appendix.Commits, *commit)
	context.appendix.CommitMap[commit.Hash] = true

	//save
	context.appendix.saveAppendix(context)
	return commit.Hash, nil
}
