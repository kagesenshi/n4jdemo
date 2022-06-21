// define unique constraints
CREATE CONSTRAINT UniqueCompany ON (c:Company) ASSERT c.company_masked IS UNIQUE;
CREATE CONSTRAINT UniquePerson ON (p:Person) ASSERT p.person_masked IS UNIQUE;
CREATE CONSTRAINT UniqueDate ON (d:Date) ASSERT d.date IS UNIQUE;

// load main entities
LOAD CSV WITH HEADERS FROM "file:///dataset/com_rel_simulated.csv" AS row
MERGE (c:Company {company_masked: row.Comp1_masked, company_type: row.Comp1Type})
RETURN count(c);

LOAD CSV WITH HEADERS FROM "file:///dataset/com_rel_simulated.csv" AS row
MERGE (c:Company {company_masked: row.Comp2_masked, company_type: row.Comp2Type})
RETURN count(c);

LOAD CSV WITH HEADERS FROM "file:///dataset/pro_simulated.csv" AS row
MERGE (p:Person {person_masked: row.person_masked, yearborn: row.yearborn})
RETURN count(p);

LOAD CSV WITH HEADERS FROM "file:///dataset/pro_simulated.csv" AS row
MATCH (p:Person {person_masked: row.person_masked})
MATCH (c:Company {company_masked: row.company_masked})
CREATE (pf:Profession {title: row.title, 
                      startdate: CASE WHEN row.startdate = "NA" THEN NULL ELSE date(row.startdate) END,
                      enddate: CASE WHEN row.enddate = "NA" THEN NULL ELSE date(row.enddate) END,
                      profunctionname: row.profunctionname,
                      keyexecflag: row.keyexecflag,
                      topkeyexecflag: row.topkeyexecflag,
                      boardflag: row.boardflag,
                      advisorflag: row.advisorflag,
                      onlyoneflag: row.advisorflag})
MERGE (p)-[:HOLD_POSITION]->(pf)-[:POSITION_AT]->(c)
RETURN count(p);

MATCH (pf:Profession)
WHERE pf.startdate IS NOT NULL
MERGE (d:Date {date: pf.startdate})
MERGE (pf)-[:START_ON]->(d)
RETURN count(d);

MATCH (pf:Profession)
WHERE pf.enddate IS NOT NULL
MERGE (d:Date {date: pf.enddate})
MERGE (pf)-[:END_ON]->(d)
RETURN count(d);

LOAD CSV WITH HEADERS FROM "file:///dataset/stockprice_simulated.csv" AS row
MERGE (sp:StockPrice {
        date: date(row.date),
        company_masked: row.company_masked,
        market_cap: row.market_cap,
        closing_price: row.closing_price
})
RETURN count(sp);

create btree index StockPriceCompanyDate for (n:StockPrice) on (n.date, n.company_masked);

MATCH (sp:StockPrice)
WHERE sp.date IS NOT NULL
MERGE (d:Date {date: sp.date})
MERGE (sp)-[:PRICE_ON]->(d)
RETURN count(d);



LOAD CSV WITH HEADERS FROM "file:///dataset/shareholding_simulated.csv" AS row
MATCH (p:Person {person_masked: row.shareholder_masked})
MATCH (c:Company {company_masked: row.company_masked})
CREATE (s:Share {
        shareholder_masked: row.shareholder_masked,
        sh_type: row.sh_type,
        periodstartdate: CASE WHEN row.periodstartdate = "NA" THEN NULL ELSE date(row.periodstartdate) END,
        periodenddate: CASE WHEN row.periodenddate = "NA" THEN NULL ELSE date(row.periodenddate) END,
        holdingdate: CASE WHEN row.holdingdate = "NA" THEN NULL ELSE date(row.holdingdate) END,
        sharesheld: row.sharesheld,
        optionsheld: row.optionsheld,
        percentofsharesoutstanding: row.percentofsharesoutstanding,
        shareschanged: row.shareschanged,
        percentshareschanged: row.percentshareschanged,
        ranksharesheld: row.ranksharesheld
})
MERGE (p)-[r:OWN_SHARE]->(s)
MERGE (s)-[l:SHARE_OF]->(c)
RETURN count(s);


LOAD CSV WITH HEADERS FROM "file:///dataset/shareholding_simulated.csv" AS row
MATCH (p:Company {company_masked: row.shareholder_masked})
MATCH (c:Company {company_masked: row.company_masked})
CREATE (s:Share {
        shareholder_masked: row.shareholder_masked,
        sh_type: row.sh_type,
        periodstartdate: CASE WHEN row.periodstartdate = "NA" THEN NULL ELSE date(row.periodstartdate) END,
        periodenddate: CASE WHEN row.periodenddate = "NA" THEN NULL ELSE date(row.periodenddate) END,
        holdingdate: CASE WHEN row.holdingdate = "NA" THEN NULL ELSE date(row.holdingdate) END,
        sharesheld: row.sharesheld,
        optionsheld: row.optionsheld,
        percentofsharesoutstanding: row.percentofsharesoutstanding,
        shareschanged: row.shareschanged,
        percentshareschanged: row.percentshareschanged,
        ranksharesheld: row.ranksharesheld
})
MERGE (p)-[r:OWN_SHARE]->(s)
MERGE (s)-[l:SHARE_OF]->(c)
RETURN count(s);

MATCH (s:Share)
WHERE s.periodstartdate IS NOT NULL
MERGE (d:Date {date: s.periodstartdate})
MERGE (s)-[:START_ON]->(d)
RETURN count(d);


MATCH (s:Share)
WHERE s.periodenddate IS NOT NULL
MERGE (d:Date {date: s.periodenddate})
MERGE (s)-[:END_ON]->(d)
RETURN count(d);

MATCH (s:Share)
WHERE s.holdingdate IS NOT NULL
MERGE (d:Date {date: s.holdingdate})
MERGE (s)-[:HELD_ON]->(d)
RETURN count(d);

LOAD CSV WITH HEADERS FROM "file:///dataset/com_rel_simulated.csv" AS row
MATCH (c1:Company {company_masked: row.Comp1_masked})
MATCH (c2:Company {company_masked: row.Comp2_masked})
MERGE (c1)-[r:C2C_RELATION {
    currentflag: row.currentflag,
    percentownership: row.percentownership,
    priorflag: row.priorflag,
    totalinvestment: row.totalinvestment,
    companyreltypename: row.companyreltypename
}]->(c2)
RETURN count(r);
