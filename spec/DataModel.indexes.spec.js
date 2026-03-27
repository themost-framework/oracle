import { createIndexes } from './createIndexes';
import { TestApplication } from './TestApplication';

describe('StringFunctions', () => {
    /**
     * @type {TestApplication}
     */
    let app;
    let context;
    beforeAll(async () => {
        app = new TestApplication(__dirname);
        await app.trySetData();
        
    });
    beforeEach(async () => {
        context = app.createContext();
    });
    afterAll(async () => {
        await app.finalizeAsync();
    });
    afterEach(async () => {
        await context.finalizeAsync();
    });

    it('should update or create indexes', async () => {
        await createIndexes(context, 'Person');
    });

});