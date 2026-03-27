/**
 * Drop and create indexes for the given data model
 * @param {import('@themost/data').DataContext} context 
 * @param {string} model 
 */
async function createIndexes(context, model) {
    await context.db.executeInTransactionAsync(async () => {
        const target = context.model(model);
        const { fields, attributes, sourceAdapter: table } = target;
        // get indexes from associations
        const associationIndexes = attributes.filter((attribute) => {
            return fields.findIndex((x) => x.name === attribute.name) > 0;
        }).filter((attribute) => {
            return !attribute.indexed;
        }).filter((attribute) => {
            return !attribute.many;
        }).filter((attribute) => {
            const mapping = target.inferMapping(attribute.name);
            return mapping && mapping.associationType === 'association';
        }).map((attribute) => {
            return {
                name: `INDEX_${table.toUpperCase()}_${attribute.name.toUpperCase()}`,
                columns: [
                    attribute.name
                ]
            }
        });
        // get other attributes that aremarked as indexed
        const otherIndexes = attributes.filter((attribute) => {
            return fields.findIndex((x) => x.name === attribute.name) > 0;
        }).filter((attribute) => {
            return attribute.indexed;
        }).filter((attribute) => {
            return !attribute.many;
        }).map((attribute) => {
            return {
                name: `INDEX_${table.toUpperCase()}_${attribute.name.toUpperCase()}`,
                columns: [
                    attribute.name
                ]
            }
        });
        associationIndexes.push(...otherIndexes);
        const indexes = context.db.indexes(table);
        for (const associationIndex of associationIndexes) {
            await indexes.dropAsync(associationIndex.name);
            await indexes.createAsync(associationIndex.name, associationIndex.columns);
        }
    });
}

module.exports = {
    createIndexes
}