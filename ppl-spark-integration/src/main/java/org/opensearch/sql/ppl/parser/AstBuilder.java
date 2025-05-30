/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParser;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParser.FillNullWithFieldVariousValuesContext;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParser.FillNullWithTheSameValueContext;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.AttributeList;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.tree.FieldSummary;
import org.opensearch.sql.ast.expression.FieldsMapping;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Scope;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.*;
import org.opensearch.sql.ast.tree.FillNull.NullableFieldFill;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Correlation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.DescribeRelation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareAggregation;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.TopAggregation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.utils.ArgumentFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.opensearch.sql.ast.tree.FillNull.ContainNullableFieldFill.ofSameValue;
import static org.opensearch.sql.ast.tree.FillNull.ContainNullableFieldFill.ofVariousValue;


/** Class of building the AST. Refines the visit path and build the AST nodes */
public class AstBuilder extends OpenSearchPPLParserBaseVisitor<UnresolvedPlan> {

  private AstExpressionBuilder expressionBuilder;

  /**
   * PPL query to get original token text. This is necessary because token.getText() returns text
   * without whitespaces or other characters discarded by lexer.
   */
  private String query;

  public AstBuilder(String query) {
    this.expressionBuilder = new AstExpressionBuilder(this);
    this.query = query;
  }

  @Override
  public UnresolvedPlan visitQueryStatement(OpenSearchPPLParser.QueryStatementContext ctx) {
    UnresolvedPlan pplCommand = visit(ctx.pplCommands());
    return ctx.commands().stream().map(this::visit).reduce(pplCommand, (r, e) -> e.attach(r));
  }

  @Override
  public UnresolvedPlan visitSubSearch(OpenSearchPPLParser.SubSearchContext ctx) {
    UnresolvedPlan searchCommand = visit(ctx.searchCommand());
    return ctx.commands().stream().map(this::visit).reduce(searchCommand, (r, e) -> e.attach(r));
  }

  /** Search command. */
  @Override
  public UnresolvedPlan visitSearchFrom(OpenSearchPPLParser.SearchFromContext ctx) {
    return visitFromClause(ctx.fromClause());
  }

  @Override
  public UnresolvedPlan visitSearchFromFilter(OpenSearchPPLParser.SearchFromFilterContext ctx) {
    return new Filter(internalVisitExpression(ctx.logicalExpression()))
        .attach(visit(ctx.fromClause()));
  }

  @Override
  public UnresolvedPlan visitSearchFilterFrom(OpenSearchPPLParser.SearchFilterFromContext ctx) {
    return new Filter(internalVisitExpression(ctx.logicalExpression()))
        .attach(visit(ctx.fromClause()));
  }

  @Override
  public UnresolvedPlan visitDescribeCommand(OpenSearchPPLParser.DescribeCommandContext ctx) {
    final Relation table = (Relation) visitTableSourceClause(ctx.tableSourceClause());
    QualifiedName tableQualifiedName = table.getTableQualifiedName();
    ArrayList<String> parts = new ArrayList<>(tableQualifiedName.getParts());
    return new DescribeRelation(new QualifiedName(parts));
  }

  /** Where command. */
  @Override
  public UnresolvedPlan visitWhereCommand(OpenSearchPPLParser.WhereCommandContext ctx) {
    return new Filter(internalVisitExpression(ctx.logicalExpression()));
  }

  @Override
  public UnresolvedPlan visitExpandCommand(OpenSearchPPLParser.ExpandCommandContext ctx) {
    return new Expand((Field) internalVisitExpression(ctx.fieldExpression()),
            ctx.alias!=null ? Optional.of(internalVisitExpression(ctx.alias)) : Optional.empty());
  }

  @Override
  public UnresolvedPlan visitCorrelateCommand(OpenSearchPPLParser.CorrelateCommandContext ctx) {
    return new Correlation(ctx.correlationType().getText(),
            ctx.fieldList().fieldExpression().stream()
                    .map(OpenSearchPPLParser.FieldExpressionContext::qualifiedName)
                    .map(this::internalVisitExpression)
                    .map(u -> (QualifiedName) u)
                    .collect(Collectors.toList()),
            Objects.isNull(ctx.scopeClause()) ? null : new Scope(expressionBuilder.visit(ctx.scopeClause().fieldExpression()),
                     expressionBuilder.visit(ctx.scopeClause().value), 
                     SpanUnit.of(Objects.isNull(ctx.scopeClause().unit) ? "" : ctx.scopeClause().unit.getText())),
            Objects.isNull(ctx.mappingList()) ? new FieldsMapping(emptyList()) : new FieldsMapping(ctx.mappingList()
                    .mappingClause().stream()
                    .map(this::internalVisitExpression)
                    .collect(Collectors.toList())));
  }

  @Override
  public UnresolvedPlan visitJoinCommand(OpenSearchPPLParser.JoinCommandContext ctx) {
    Join.JoinType joinType = getJoinType(ctx.joinType());
    if (ctx.joinCriteria() == null) {
      joinType = Join.JoinType.CROSS;
    }
    Join.JoinHint joinHint = getJoinHint(ctx.joinHintList());
    Optional<String> leftAlias = ctx.sideAlias().leftAlias != null ? Optional.of(internalVisitExpression(ctx.sideAlias().leftAlias).toString()) : Optional.empty();
    Optional<String> rightAlias = Optional.empty();
    if (ctx.tableOrSubqueryClause().alias != null) {
      rightAlias = Optional.of(internalVisitExpression(ctx.tableOrSubqueryClause().alias).toString());
    }
    if (ctx.sideAlias().rightAlias != null) {
      rightAlias = Optional.of(internalVisitExpression(ctx.sideAlias().rightAlias).toString());
    }
    // "JOIN on id = uid table1,table2" are not allowed
    // "JOIN on id = uid table1,table2 as t2" are not allowed
    // "JOIN on id = uid [ source = table1,table2 ]" are allowed
    if (ctx.tableOrSubqueryClause().subSearch() == null
            && ctx.tableOrSubqueryClause().tableSourceClause().tableSource().size() > 1) {
      UnresolvedPlan plan = visit(ctx.tableOrSubqueryClause());
      Relation relation = null;
      if (plan instanceof Relation) {
        relation = (Relation) plan;
      } else if (plan instanceof SubqueryAlias) {
        relation = (Relation)((SubqueryAlias) plan).getChild().get(0);
      }
      throw new SyntaxCheckException("Join command only support two tables."
        + (relation == null ? "" : " But got " + relation.getQualifiedNames()));
    }
    UnresolvedPlan rightRelation = visit(ctx.tableOrSubqueryClause());
    // Add a SubqueryAlias to the right plan when the right alias is present and no duplicated alias existing in right.
    UnresolvedPlan right;
    if (rightAlias.isEmpty() ||
        (rightRelation instanceof SubqueryAlias &&
            rightAlias.get().equals(((SubqueryAlias) rightRelation).getAlias()))) {
      right = rightRelation;
    } else {
      right = new SubqueryAlias(rightAlias.get(), rightRelation);
    }
    Optional<UnresolvedExpression> joinCondition =
        ctx.joinCriteria() == null ? Optional.empty() : Optional.of(expressionBuilder.visitJoinCriteria(ctx.joinCriteria()));

    return new Join(right, leftAlias, rightAlias, joinType, joinCondition, joinHint);
  }

  private Join.JoinHint getJoinHint(OpenSearchPPLParser.JoinHintListContext ctx) {
    Join.JoinHint joinHint;
    if (ctx == null) {
      joinHint = new Join.JoinHint();
    } else {
      joinHint = new Join.JoinHint(
          ctx.hintPair().stream()
              .map(pCtx -> expressionBuilder.visit(pCtx))
              .filter(e -> e instanceof EqualTo)
              .map(e -> (EqualTo) e)
              .collect(Collectors.toMap(
                  k -> k.getLeft().toString(), // always literal
                  v -> v.getRight().toString(), // always literal
                  (v1, v2) -> v2,
                  LinkedHashMap::new)));
    }
    return joinHint;
  }

  private Join.JoinType getJoinType(OpenSearchPPLParser.JoinTypeContext ctx) {
    Join.JoinType joinType;
    if (ctx == null) {
      joinType = Join.JoinType.INNER;
    } else if (ctx.INNER() != null) {
      joinType = Join.JoinType.INNER;
    } else if (ctx.SEMI() != null) {
      joinType = Join.JoinType.SEMI;
    } else if (ctx.ANTI() != null) {
      joinType = Join.JoinType.ANTI;
    } else if (ctx.LEFT() != null) {
      joinType = Join.JoinType.LEFT;
    } else if (ctx.RIGHT() != null) {
      joinType = Join.JoinType.RIGHT;
    } else if (ctx.CROSS() != null) {
      joinType = Join.JoinType.CROSS;
    } else if (ctx.FULL() != null) {
      joinType = Join.JoinType.FULL;
    } else {
      joinType = Join.JoinType.INNER;
    }
    return joinType;
  }

  /** Fields command. */
  @Override
  public UnresolvedPlan visitFieldsCommand(OpenSearchPPLParser.FieldsCommandContext ctx) {
    return new Project(
        ctx.fieldList().fieldExpression().stream()
            .map(this::internalVisitExpression)
            .collect(Collectors.toList()),
        ArgumentFactory.getArgumentList(ctx));
  }

  /** Rename command. */
  @Override
  public UnresolvedPlan visitRenameCommand(OpenSearchPPLParser.RenameCommandContext ctx) {
    return new Rename(
        ctx.renameClasue().stream()
            .map(
                ct ->
                    new Alias(
                        ((Field) internalVisitExpression(ct.renamedField)).getField().toString(),
                        internalVisitExpression(ct.orignalField)))
            .collect(Collectors.toList()));
  }

  /** Stats command. */
  @Override
  public UnresolvedPlan visitStatsCommand(OpenSearchPPLParser.StatsCommandContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> aggListBuilder = new ImmutableList.Builder<>();
    for (OpenSearchPPLParser.StatsAggTermContext aggCtx : ctx.statsAggTerm()) {
      UnresolvedExpression aggExpression = internalVisitExpression(aggCtx.statsFunction());
      String name =
          aggCtx.alias == null
              ? getTextInQuery(aggCtx)
              : ((Field) internalVisitExpression(aggCtx.alias)).getField().toString();
      Alias alias = new Alias(name, aggExpression);
      aggListBuilder.add(alias);
    }

    List<UnresolvedExpression> groupList =
        Optional.ofNullable(ctx.statsByClause())
            .map(OpenSearchPPLParser.StatsByClauseContext::fieldList)
            .map(
                expr ->
                    expr.fieldExpression().stream()
                        .map(
                            groupCtx ->
                                (UnresolvedExpression)
                                    new Alias(
                                        getTextInQuery(groupCtx),
                                        internalVisitExpression(groupCtx)))
                        .collect(Collectors.toList()))
            .orElse(emptyList());

    UnresolvedExpression span =
        Optional.ofNullable(ctx.statsByClause())
            .map(OpenSearchPPLParser.StatsByClauseContext::bySpanClause)
            .map(this::internalVisitExpression)
            .orElse(null);

    if (ctx.STATS() != null) {
      Aggregation aggregation =
          new Aggregation(
              aggListBuilder.build(),
              emptyList(),
              groupList,
              span,
              ArgumentFactory.getArgumentList(ctx));
      return aggregation;
    } else {
      Window window =
          new Window(
              aggListBuilder.build(),
              groupList,
              emptyList());
      window.setSpan(span);
      return window;
    }
  }

  /** Dedup command. */
  @Override
  public UnresolvedPlan visitDedupCommand(OpenSearchPPLParser.DedupCommandContext ctx) {
    return new Dedupe(ArgumentFactory.getArgumentList(ctx), getFieldList(ctx.fieldList()));
  }

  /** Head command visitor. */
  @Override
  public UnresolvedPlan visitHeadCommand(OpenSearchPPLParser.HeadCommandContext ctx) {
    Integer size = ctx.number != null ? Integer.parseInt(ctx.number.getText()) : 10;
    Integer from = ctx.from != null ? Integer.parseInt(ctx.from.getText()) : 0;
    return new Head(size, from);
  }

  /** Sort command. */
  @Override
  public UnresolvedPlan visitSortCommand(OpenSearchPPLParser.SortCommandContext ctx) {
    return new Sort(
        ctx.sortbyClause().sortField().stream()
            .map(sort -> (Field) internalVisitExpression(sort))
            .collect(Collectors.toList()));
  }

  /** Eval command. */
  @Override
  public UnresolvedPlan visitEvalCommand(OpenSearchPPLParser.EvalCommandContext ctx) {
    return new Eval(
        ctx.evalClause().stream()
            .map(ct -> (ct.geoipCommand() != null) ? visit(ct.geoipCommand()) : (Let) internalVisitExpression(ct))
            .collect(Collectors.toList()));
  }

  @Override
  public UnresolvedPlan visitGeoipCommand(OpenSearchPPLParser.GeoipCommandContext ctx) {
    Field field = (Field) internalVisitExpression(ctx.fieldExpression());
    UnresolvedExpression ipAddress = internalVisitExpression(ctx.ipAddress);
    AttributeList properties = ctx.properties == null ? new AttributeList(Collections.emptyList()) : (AttributeList) internalVisitExpression(ctx.properties);
    return new GeoIp(field, ipAddress, properties);
  }

  private List<UnresolvedExpression> getGroupByList(OpenSearchPPLParser.ByClauseContext ctx) {
    return ctx.fieldList().fieldExpression().stream()
        .map(this::internalVisitExpression)
        .collect(Collectors.toList());
  }

  private List<Field> getFieldList(OpenSearchPPLParser.FieldListContext ctx) {
    return ctx.fieldExpression().stream()
        .map(field -> (Field) internalVisitExpression(field))
        .collect(Collectors.toList());
  }

  @Override
  public UnresolvedPlan visitGrokCommand(OpenSearchPPLParser.GrokCommandContext ctx) {
    UnresolvedExpression sourceField = internalVisitExpression(ctx.source_field);
    Literal pattern = (Literal) internalVisitExpression(ctx.pattern);

    return new Parse(ParseMethod.GROK, sourceField, pattern, ImmutableMap.of());
  }

  @Override
  public UnresolvedPlan visitParseCommand(OpenSearchPPLParser.ParseCommandContext ctx) {
    UnresolvedExpression sourceField = internalVisitExpression(ctx.source_field);
    Literal pattern = (Literal) internalVisitExpression(ctx.pattern);

    return new Parse(ParseMethod.REGEX, sourceField, pattern, ImmutableMap.of());
  }

  @Override
  public UnresolvedPlan visitPatternsCommand(OpenSearchPPLParser.PatternsCommandContext ctx) {
    UnresolvedExpression sourceField = internalVisitExpression(ctx.source_field);
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    ctx.patternsParameter()
        .forEach(
            x -> {
              builder.put(
                  x.children.get(0).toString(),
                  (Literal) internalVisitExpression(x.children.get(2)));
            });
    java.util.Map<String, Literal> arguments = builder.build();
    Literal pattern = arguments.getOrDefault("pattern", new Literal("", DataType.STRING));

    return new Parse(ParseMethod.PATTERNS, sourceField, pattern, arguments);
  }

  /** Lookup command */
  @Override
  public UnresolvedPlan visitLookupCommand(OpenSearchPPLParser.LookupCommandContext ctx) {
    Relation lookupRelation = new Relation(Collections.singletonList(this.internalVisitExpression(ctx.tableSource())));
    Lookup.OutputStrategy strategy =
        ctx.APPEND() != null ? Lookup.OutputStrategy.APPEND : Lookup.OutputStrategy.REPLACE;
    java.util.Map<Alias, Field> lookupMappingList = buildLookupPair(ctx.lookupMappingList().lookupPair());
    java.util.Map<Alias, Field> outputCandidateList =
        ctx.APPEND() == null && ctx.REPLACE() == null ? emptyMap() : buildLookupPair(ctx.outputCandidateList().lookupPair());
    return new Lookup(new SubqueryAlias(lookupRelation, "_l"), lookupMappingList, strategy, outputCandidateList);
  }

  private java.util.Map<Alias, Field> buildLookupPair(List<OpenSearchPPLParser.LookupPairContext> ctx) {
    return ctx.stream()
      .map(of -> expressionBuilder.visitLookupPair(of))
      .map(And.class::cast)
      .collect(Collectors.toMap(and -> (Alias) and.getLeft(), and -> (Field) and.getRight(), (x, y) -> y, LinkedHashMap::new));
  }

  @Override
  public UnresolvedPlan visitTrendlineCommand(OpenSearchPPLParser.TrendlineCommandContext ctx) {
    List<Trendline.TrendlineComputation> trendlineComputations = ctx.trendlineClause()
            .stream()
            .map(this::toTrendlineComputation)
            .collect(Collectors.toList());
    return Optional.ofNullable(ctx.sortField())
            .map(this::internalVisitExpression)
            .map(Field.class::cast)
            .map(sort -> new Trendline(Optional.of(sort), trendlineComputations))
            .orElse(new Trendline(Optional.empty(), trendlineComputations));
  }

  @Override
  public UnresolvedPlan visitAppendcolCommand(OpenSearchPPLParser.AppendcolCommandContext ctx) {
    final Optional<UnresolvedPlan> pplCmd = ctx.commands().stream()
            .map(this::visit)
            .reduce((r, e) -> e.attach(r));
    final boolean override = (ctx.override != null &&
            Boolean.parseBoolean(ctx.override.getText()));
    // ANTLR parser check guarantee pplCmd won't be null.
    return new AppendCol(pplCmd.get(), override);
  }

  private Trendline.TrendlineComputation toTrendlineComputation(OpenSearchPPLParser.TrendlineClauseContext ctx) {
    int numberOfDataPoints = Integer.parseInt(ctx.numberOfDataPoints.getText());
    if (numberOfDataPoints < 1) {
      throw new SyntaxCheckException("Number of trendline data-points must be greater than or equal to 1");
    }
    Field dataField = (Field) expressionBuilder.visitFieldExpression(ctx.field);
    String alias = ctx.alias == null? dataField.getField().toString() + "_trendline" : internalVisitExpression(ctx.alias).toString();
    String computationType = ctx.trendlineType().getText();
    return new Trendline.TrendlineComputation(numberOfDataPoints, dataField, alias, Trendline.TrendlineType.valueOf(computationType.toUpperCase()));
  }

  /** Top command. */
  @Override
  public UnresolvedPlan visitTopCommand(OpenSearchPPLParser.TopCommandContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> aggListBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<UnresolvedExpression> groupListBuilder = new ImmutableList.Builder<>();
    String funcName = ctx.TOP_APPROX() != null ? "approx_count_distinct" : "count";
    ctx.fieldList().fieldExpression().forEach(field -> {
    AggregateFunction aggExpression = new AggregateFunction(funcName,internalVisitExpression(field),
              Collections.singletonList(new Argument("countParam", new Literal(1, DataType.INTEGER))));
      String name = field.qualifiedName().getText();
      Alias alias = new Alias("count_"+name, aggExpression);
      aggListBuilder.add(alias);
      // group by the `field-list` as the mandatory groupBy fields
      groupListBuilder.add(internalVisitExpression(field));
    });

    // group by the `by-clause` as the optional groupBy fields
    groupListBuilder.addAll(
            Optional.ofNullable(ctx.byClause())
                    .map(OpenSearchPPLParser.ByClauseContext::fieldList)
                    .map(
                            expr ->
                                    expr.fieldExpression().stream()
                                            .map(
                                                    groupCtx ->
                                                            (UnresolvedExpression)
                                                                    new Alias(
                                                                            getTextInQuery(groupCtx),
                                                                            internalVisitExpression(groupCtx)))
                                            .collect(Collectors.toList()))
                    .orElse(emptyList())
    );
    UnresolvedExpression expectedResults = (ctx.number != null ? internalVisitExpression(ctx.number) : null);
    return new TopAggregation(
                    Optional.ofNullable((Literal) expectedResults),
                    aggListBuilder.build(),
                    aggListBuilder.build(),
                    groupListBuilder.build());
  }

    /** Fieldsummary command. */
    @Override
    public UnresolvedPlan visitFieldsummaryCommand(OpenSearchPPLParser.FieldsummaryCommandContext ctx) {
        return new FieldSummary(ctx.fieldsummaryParameter().stream().map(arg -> expressionBuilder.visit(arg)).collect(Collectors.toList()));
    }

    /** Rare command. */
  @Override
  public UnresolvedPlan visitRareCommand(OpenSearchPPLParser.RareCommandContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> aggListBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<UnresolvedExpression> groupListBuilder = new ImmutableList.Builder<>();
    String funcName = ctx.RARE_APPROX() != null ? "approx_count_distinct" : "count";
    ctx.fieldList().fieldExpression().forEach(field -> {
      AggregateFunction aggExpression = new AggregateFunction(funcName,internalVisitExpression(field),
              Collections.singletonList(new Argument("countParam", new Literal(1, DataType.INTEGER))));
      String name = field.qualifiedName().getText();
      Alias alias = new Alias("count_"+name, aggExpression);
      aggListBuilder.add(alias);
      // group by the `field-list` as the mandatory groupBy fields
      groupListBuilder.add(internalVisitExpression(field));
    });

    // group by the `by-clause` as the optional groupBy fields
    groupListBuilder.addAll(
            Optional.ofNullable(ctx.byClause())
                    .map(OpenSearchPPLParser.ByClauseContext::fieldList)
                    .map(
                            expr ->
                                    expr.fieldExpression().stream()
                                            .map(
                                                    groupCtx ->
                                                            (UnresolvedExpression)
                                                                    new Alias(
                                                                            getTextInQuery(groupCtx),
                                                                            internalVisitExpression(groupCtx)))
                                            .collect(Collectors.toList()))
                    .orElse(emptyList())
    );
    UnresolvedExpression expectedResults = (ctx.number != null ? internalVisitExpression(ctx.number) : null);
      return new RareAggregation(
                    Optional.ofNullable((Literal) expectedResults),
                    aggListBuilder.build(),
                    aggListBuilder.build(),
                    groupListBuilder.build());
  }

  @Override
  public UnresolvedPlan visitTableOrSubqueryClause(OpenSearchPPLParser.TableOrSubqueryClauseContext ctx) {
      if (ctx.subSearch() != null) {
          return ctx.alias != null
              ? new SubqueryAlias(internalVisitExpression(ctx.alias).toString(), visitSubSearch(ctx.subSearch()))
              : visitSubSearch(ctx.subSearch());
      } else {
          return visitTableSourceClause(ctx.tableSourceClause());
      }
  }

  @Override
  public UnresolvedPlan visitTableSourceClause(OpenSearchPPLParser.TableSourceClauseContext ctx) {
    Relation relation = new Relation(ctx.tableSource().stream().map(this::internalVisitExpression).collect(Collectors.toList()));
    return ctx.alias != null ? new SubqueryAlias(internalVisitExpression(ctx.alias).toString(), relation) : relation;
  }

  @Override
  public UnresolvedPlan visitTableFunction(OpenSearchPPLParser.TableFunctionContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    ctx.functionArgs()
        .functionArg()
        .forEach(
            arg -> {
              String argName = (arg.ident() != null) ? arg.ident().getText() : null;
              builder.add(
                  new UnresolvedArgument(
                      argName, this.internalVisitExpression(arg.valueExpression())));
            });
    return new TableFunction(this.internalVisitExpression(ctx.qualifiedName()), builder.build());
  }

  /** Navigate to & build AST expression. */
  private UnresolvedExpression internalVisitExpression(ParseTree tree) {
    return expressionBuilder.visit(tree);
  }

  /** Simply return non-default value for now. */
  @Override
  protected UnresolvedPlan aggregateResult(UnresolvedPlan aggregate, UnresolvedPlan nextResult) {
    if (nextResult != defaultResult()) {
      return nextResult;
    }
    return aggregate;
  }

  /** Kmeans command. */
  @Override
  public UnresolvedPlan visitKmeansCommand(OpenSearchPPLParser.KmeansCommandContext ctx) {
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    ctx.kmeansParameter()
        .forEach(
            x -> {
              builder.put(
                  x.children.get(0).toString(),
                  (Literal) internalVisitExpression(x.children.get(2)));
            });
    return new Kmeans(builder.build());
  }

  @Override
  public UnresolvedPlan visitFillnullCommand(OpenSearchPPLParser.FillnullCommandContext ctx) {
    // ctx contain result of parsing fillnull command. Lets transform it to UnresolvedPlan which is FillNull
    FillNullWithTheSameValueContext sameValueContext = ctx.fillNullWithTheSameValue();
    FillNullWithFieldVariousValuesContext variousValuesContext = ctx.fillNullWithFieldVariousValues();
    if (sameValueContext != null) {
      // todo consider using expression instead of Literal
      UnresolvedExpression replaceNullWithMe = internalVisitExpression(sameValueContext.nullReplacement);
      List<Field> fieldsToReplace = sameValueContext.nullableFieldList.fieldExpression()
              .stream()
              .map(this::internalVisitExpression)
              .map(Field.class::cast)
              .collect(Collectors.toList());
      return new FillNull(ofSameValue(replaceNullWithMe, fieldsToReplace));
    } else if (variousValuesContext != null) {
      List<NullableFieldFill> nullableFieldFills = IntStream.range(0, variousValuesContext.nullableReplacementExpression().size())
              .mapToObj(index -> {
                UnresolvedExpression replaceNullWithMe = internalVisitExpression(variousValuesContext.nullableReplacementExpression(index).nullableReplacement);
                Field nullableFieldReference = (Field) internalVisitExpression(variousValuesContext.nullableReplacementExpression(index).nullableField);
                return new NullableFieldFill(nullableFieldReference, replaceNullWithMe);
              })
              .collect(Collectors.toList());
      return new FillNull(ofVariousValue(nullableFieldFills));
    } else {
      throw new SyntaxCheckException("Invalid fillnull command");
    }
  }

  @Override
  public UnresolvedPlan visitFlattenCommand(OpenSearchPPLParser.FlattenCommandContext ctx) {
    Field unresolvedExpression = (Field) internalVisitExpression(ctx.fieldExpression());
    List<UnresolvedExpression> alias = ctx.alias == null ? emptyList() : ((AttributeList) internalVisitExpression(ctx.alias)).getAttrList();
    return new Flatten(unresolvedExpression, alias);
  }

  /** AD command. */
  @Override
  public UnresolvedPlan visitAdCommand(OpenSearchPPLParser.AdCommandContext ctx) {
    throw new RuntimeException("AD Command is not supported ");

  }

  /** ml command. */
  @Override
  public UnresolvedPlan visitMlCommand(OpenSearchPPLParser.MlCommandContext ctx) {
    throw new RuntimeException("ML Command is not supported ");
  }

  /** Get original text in query. */
  private String getTextInQuery(ParserRuleContext ctx) {
    Token start = ctx.getStart();
    Token stop = ctx.getStop();
    return query.substring(start.getStartIndex(), stop.getStopIndex() + 1);
  }
}
