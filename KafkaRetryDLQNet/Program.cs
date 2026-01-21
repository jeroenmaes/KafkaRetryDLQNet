using KafkaRetryDLQNet;

var builder = Host.CreateApplicationBuilder(args);

// Configure Kafka settings
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));

// Register services
builder.Services.AddSingleton<EmployeeRepository>();
builder.Services.AddSingleton<MessageRouter>();

// Register background services
builder.Services.AddHostedService<TopicCreator>();
builder.Services.AddHostedService<MessageProducer>();
builder.Services.AddHostedService<MainConsumer>();
builder.Services.AddHostedService<Retry1Consumer>();
builder.Services.AddHostedService<Retry2Consumer>();
builder.Services.AddHostedService<Retry3Consumer>();

var host = builder.Build();
host.Run();
