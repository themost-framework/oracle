import { OracleFormatter } from '../index';
import { QueryExpression } from '@themost/query';
import { TestApplication } from './TestApplication';

describe('OracleAdapter', () => {
    /**
     * @type {TestApplication}
     */
    let app;
    beforeAll(async () => {
        app = new TestApplication(__dirname);
    });
    beforeEach(async () => {
        //
    });
    afterAll(async () => {
        await app.finalize();
    });
    afterEach(async () => {
        //
    });

    it('should check table', async () => {
        await app.executeInTestTransaction(async (context) => {
            const exists = await context.db.table('Table1').existsAsync();
            expect(exists).toBeFalsy();
        });
    });

    it('should create table', async () => {
        await app.executeInTestTransaction(async (context) => {
            const db = context.db;
            let exists = await db.table('Table1').existsAsync();
            expect(exists).toBeFalsy();
            await context.db.table('Table1').createAsync([
                {
                    name: 'id',
                    type: 'Counter',
                    primary: true,
                    nullable: false
                },
                {
                    name: 'name',
                    type: 'Text',
                    size: 255,
                    nullable: false
                },
                {
                    name: 'description',
                    type: 'Text',
                    size: 255,
                    nullable: true
                }
            ]);
            exists = await db.table('Table1').existsAsync();
            expect(exists).toBeTruthy();
            // get columns
            const columns = await db.table('Table1').columnsAsync();
            expect(columns).toBeInstanceOf(Array);
            let column = columns.find((col) => col.name === 'id');
            expect(column).toBeTruthy();
            expect(column.nullable).toBeFalsy();
            column = columns.find((col) => col.name === 'description');
            expect(column).toBeTruthy();
            expect(column.nullable).toBeTruthy();
            expect(column.size).toBe(255);
            const sql = `DROP TABLE ${new OracleFormatter().escapeName('Table1')}`
            await db.executeAsync(sql, null);
        });
    });

    it('should alter table', async () => {
        await app.executeInTestTransaction(async (context) => {
            const db = context.db;
            let exists = await db.table('Table2').existsAsync();
            expect(exists).toBeFalsy();
            await db.table('Table2').createAsync([
                {
                    name: 'id',
                    type: 'Counter',
                    primary: true,
                    nullable: false
                },
                {
                    name: 'name',
                    type: 'Text',
                    size: 255,
                    nullable: false
                }
            ]);
            exists = await db.table('Table2').existsAsync();
            expect(exists).toBeTruthy();
            await db.table('Table2').addAsync([
                {
                    name: 'description',
                    type: 'Text',
                    size: 255,
                    nullable: true
                }
            ]);
            // get columns
            let columns = await db.table('Table2').columnsAsync();
            expect(columns).toBeInstanceOf(Array);
            let column = columns.find((col) => col.name === 'description');
            expect(column).toBeTruthy();

            await db.table('Table2').changeAsync([
                {
                    name: 'description',
                    type: 'Text',
                    size: 512,
                    nullable: true
                }
            ]);
            columns = await db.table('Table2').columnsAsync();
            column = columns.find((col) => col.name === 'description');
            expect(column.size).toEqual(512);
            expect(column.nullable).toBeTruthy();
            await db.executeAsync(`DROP TABLE ${new OracleFormatter().escapeName('Table2')}`);
        });

    });


    it('should create view', async () => {

        await app.executeInTestTransaction(async (context) => {
            const db = context.db;
            let exists = await db.table('Table1').existsAsync();
            expect(exists).toBeFalsy();
            await db.table('Table1').createAsync([
                {
                    name: 'id',
                    type: 'Counter',
                    primary: true,
                    nullable: false
                },
                {
                    name: 'name',
                    type: 'Text',
                    size: 255,
                    nullable: false
                },
                {
                    name: 'description',
                    type: 'Text',
                    size: 255,
                    nullable: true
                }
            ]);
            exists = await db.table('Table1').existsAsync();
            expect(exists).toBeTruthy();

            exists = await db.view('View1').existsAsync();
            expect(exists).toBeFalsy();

            const query = new QueryExpression().select('id', 'name', 'description').from('Table1');
            await db.view('View1').createAsync(query);

            exists = await db.view('View1').existsAsync();
            expect(exists).toBeTruthy();

            await db.view('View1').dropAsync();

            exists = await db.view('View1').existsAsync();
            expect(exists).toBeFalsy();
        });
    });

    it('should create index', async () => {
        await app.executeInTestTransaction(async (context) => {
            const db = context.db;
            let exists = await db.table('Table1').existsAsync();
            expect(exists).toBeFalsy();
            await db.table('Table1').createAsync([
                {
                    name: 'id',
                    type: 'Counter',
                    primary: true,
                    nullable: false
                },
                {
                    name: 'name',
                    type: 'Text',
                    size: 255,
                    nullable: false
                },
                {
                    name: 'description',
                    type: 'Text',
                    size: 255,
                    nullable: true
                }
            ]);
            exists = await db.table('Table1').existsAsync();
            expect(exists).toBeTruthy();

            let list = await db.indexes('Table1').listAsync();
            expect(list).toBeInstanceOf(Array);
            exists = list.findIndex((index) => index.name === 'idx_name') < 0;

            await db.indexes('Table1').createAsync('idx_name', [
                'name'
            ]);

            list = await db.indexes('Table1').listAsync();
            expect(list).toBeInstanceOf(Array);
            exists = list.findIndex((index) => index.name === 'idx_name') >= 0;
            expect(exists).toBeTruthy();

            await db.indexes('Table1').dropAsync('idx_name');

            list = await db.indexes('Table1').listAsync();
            expect(list).toBeInstanceOf(Array);
            exists = list.findIndex((index) => index.name === 'idx_name') >= 0;
            expect(exists).toBeFalsy();

            await db.executeAsync(`DROP TABLE ${new OracleFormatter().escapeName('Table1')}`);
        });
    });

    it('should should list tables', async () => {
        await app.executeInTestTransaction(async (context) => {
            const db = context.db;
            const tables = await db.tables().listAsync();
            expect(tables).toBeInstanceOf(Array);
            expect(tables.length).toBeTruthy();
            const table = tables.find((item) => item.name === 'ThingBase');
            expect(table).toBeTruthy();
        });
    });

    it('should should list views', async () => {
        await app.executeInTestTransaction(async (context) => {
            const db = context.db;
            const views = await db.views().listAsync();
            expect(views).toBeInstanceOf(Array);
            expect(views.length).toBeTruthy();
            const view = views.find((item) => item.name === 'ThingData');
            expect(view).toBeTruthy();
        });
    });
});